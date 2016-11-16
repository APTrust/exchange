package workers

import (
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/dpn/network"
	"github.com/APTrust/exchange/validation"
	"github.com/nsqio/go-nsq"
	//	"os"
)

// dpn_packager repackages APTrust bags as DPN bags so they
// can be copied into DPN.

type DPNPackager struct {
	PackageChannel      chan *models.DPNIngestManifest
	ValidationChannel   chan *models.DPNIngestManifest
	PostProcessChannel  chan *models.DPNIngestManifest
	BagValidationConfig *validation.BagValidationConfig
	Context             *context.Context
	LocalClient         *network.DPNRestClient
	RemoteClients       map[string]*network.DPNRestClient
}

func NewDPNPackager(_context *context.Context) (*DPNPackager, error) {
	localClient, err := network.NewDPNRestClient(
		_context.Config.DPN.RestClient.LocalServiceURL,
		_context.Config.DPN.RestClient.LocalAPIRoot,
		_context.Config.DPN.RestClient.LocalAuthToken,
		_context.Config.DPN.LocalNode,
		_context.Config.DPN)
	if err != nil {
		return nil, fmt.Errorf("Error creating local DPN REST client: %v", err)
	}
	remoteClients, err := localClient.GetRemoteClients()
	if err != nil {
		return nil, err
	}
	packager := &DPNPackager{
		Context:       _context,
		LocalClient:   localClient,
		RemoteClients: remoteClients,
	}
	packager.BagValidationConfig = LoadBagValidationConfig(packager.Context)
	workerBufferSize := _context.Config.DPN.DPNPackageWorker.Workers * 4
	packager.PackageChannel = make(chan *models.DPNIngestManifest, workerBufferSize)
	packager.ValidationChannel = make(chan *models.DPNIngestManifest, workerBufferSize)
	packager.PostProcessChannel = make(chan *models.DPNIngestManifest, workerBufferSize)
	for i := 0; i < _context.Config.DPN.DPNPackageWorker.Workers; i++ {
		go packager.buildBag()
		go packager.validate()
		go packager.postProcess()
	}
	return packager, nil
}

func (packager *DPNPackager) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()

	packager.Context.MessageLog.Info("Packager is checking NSQ message %s", string(message.Body))

	manifest := SetupIngestManifest(message, "package", packager.Context)
	manifest.PackageSummary.Start()
	manifest.PackageSummary.Attempted = true
	manifest.PackageSummary.AttemptNumber += 1

	// Handle the case where we cannot get the WorkItem whose id
	// is specified in the NSQ message.
	if manifest.PackageSummary.HasErrors() {
		packager.Context.MessageLog.Info("Cannot process NSQ message %s", string(message.Body))
		packager.PostProcessChannel <- manifest
		return nil
	}

	// Start processing.
	packager.Context.MessageLog.Info("Putting bag %s into the package channel",
		manifest.WorkItem.ObjectIdentifier)
	packager.PackageChannel <- manifest
	return nil
}

func (packager *DPNPackager) buildBag() {
	for manifest := range packager.PackageChannel {
		// Create the manifest.LocalDir, with data subdir
		// Create a new BagBuilder
		// Fetch all of the IntelObj's GenericFile into the data subdir
		// Add each file to the BagBuilder
		// Add manifests and other required files
		packager.ValidationChannel <- manifest
	}
}

func (packager *DPNPackager) validate() {
	for manifest := range packager.ValidationChannel {
		// Validate the bag
		// Tar it up
		// Send it into the PostProcessChannel
		packager.PostProcessChannel <- manifest
	}
}

func (packager *DPNPackager) postProcess() {
	for manifest := range packager.PostProcessChannel {
		if manifest.PackageSummary.HasErrors() {
			packager.finishWithError(manifest)
		} else {
			packager.finishWithSuccess(manifest)
		}
	}
}

func (packager *DPNPackager) createBagDirectory(manifest *models.DPNIngestManifest) {
	if manifest.LocalDir == "" {
		manifest.PackageSummary.AddError("LocalDirectory is not set for bag %s",
			manifest.IntellectualObject.Identifier)
		manifest.PackageSummary.ErrorIsFatal = true
		manifest.PackageSummary.Retry = false
		return
	}
}

func (packager *DPNPackager) finishWithSuccess(manifest *models.DPNIngestManifest) {

}

func (packager *DPNPackager) finishWithError(manifest *models.DPNIngestManifest) {

}
