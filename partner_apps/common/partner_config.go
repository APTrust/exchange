package common

import (
	"bufio"
	"fmt"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/fileutil"
	"io"
	"os"
	"strings"
)

type PartnerConfig struct {
	AwsAccessKeyId     string
	AwsSecretAccessKey string
	ReceivingBucket    string
	RestorationBucket  string
	DownloadDir        string
	warnings           []string
}

func LoadPartnerConfig(configFile string) (*PartnerConfig, error) {
	file, err := os.Open(configFile)
	if err != nil {
		return nil, fmt.Errorf("Cannot open config file: %v", err)
	}
	defer file.Close()
	return parsePartnerConfig(file)
}

func parsePartnerConfig(file *os.File) (*PartnerConfig, error) {
	partnerConfig := &PartnerConfig{
		warnings: make([]string, 0),
	}
	bufReader := bufio.NewReader(file)
	lineNum := 0
	for {
		lineNum++
		bytes, _, err := bufReader.ReadLine()
		line := string(bytes)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		cleanLine := strings.TrimSpace(line)
		if cleanLine == "" || strings.HasPrefix(cleanLine, "#") {
			continue
		}
		parts := strings.SplitN(cleanLine, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("Line %d is not valid. It should contain "+
				"a #comment or name=value setting.\n"+
				"Actual line: %s", lineNum, cleanLine)
		} else {
			partnerConfig.addSetting(parts[0], parts[1])
		}
	}
	partnerConfig.ExpandFilePaths()
	return partnerConfig, nil
}

func (partnerConfig *PartnerConfig) addSetting(name, value string) {
	cleanName := util.CleanString(name)
	cleanValue := util.CleanString(value)
	switch strings.ToLower(cleanName) {
	case "awsaccesskeyid":
		partnerConfig.AwsAccessKeyId = cleanValue
	case "awssecretaccesskey":
		partnerConfig.AwsSecretAccessKey = cleanValue
	case "receivingbucket":
		partnerConfig.ReceivingBucket = cleanValue
	case "restorationbucket":
		partnerConfig.RestorationBucket = cleanValue
	case "downloaddir":
		partnerConfig.DownloadDir = cleanValue
	default:
		partnerConfig.addWarning(fmt.Sprintf("Invalid setting: %s = %s", cleanName, cleanValue))
	}
}

func (partnerConfig *PartnerConfig) addWarning(message string) {
	partnerConfig.warnings = append(partnerConfig.warnings, message)
}

func (partnerConfig *PartnerConfig) Warnings() []string {
	warnings := make([]string, len(partnerConfig.warnings))
	copy(warnings, partnerConfig.warnings)
	if partnerConfig.AwsAccessKeyId == "" && os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		warnings = append(warnings,
			"AwsAccessKeyId is missing. This setting is required only for copying files "+
				"to and from S3. You may set this in the environment instead of in the config file "+
				"if you prefer.")
	}
	if partnerConfig.AwsSecretAccessKey == "" && os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		warnings = append(warnings,
			"AwsSecretAccessKey is missing. This setting is required only for copying files "+
				"to and from S3. You may set this in the environment instead of in the config file "+
				"if you prefer.")
	}
	if partnerConfig.ReceivingBucket == "" {
		warnings = append(warnings,
			"ReceivingBucket is missing. This setting is required for uploading files to S3.")
	}
	if partnerConfig.RestorationBucket == "" {
		warnings = append(warnings,
			"RestorationBucket is missing. This setting is required for downloading restored files from S3.")
	}
	if partnerConfig.DownloadDir == "" {
		warnings = append(warnings,
			"DownloadDir is missing. This setting is required for downloading restored files from S3.")
	}
	return warnings
}

// Fill in AWS values if their missing from config file
// but present in the environment.
func (partnerConfig *PartnerConfig) LoadAwsFromEnv() {
	if partnerConfig.AwsAccessKeyId == "" && os.Getenv("AWS_ACCESS_KEY_ID") != "" {
		partnerConfig.AwsAccessKeyId = os.Getenv("AWS_ACCESS_KEY_ID")
	}
	if partnerConfig.AwsSecretAccessKey == "" && os.Getenv("AWS_SECRET_ACCESS_KEY") != "" {
		partnerConfig.AwsSecretAccessKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}
}

func (partnerConfig *PartnerConfig) Validate() error {
	partnerConfig.ExpandFilePaths()
	if partnerConfig.AwsAccessKeyId == "" || partnerConfig.AwsSecretAccessKey == "" {
		partnerConfig.LoadAwsFromEnv()
	}
	if partnerConfig.AwsAccessKeyId == "" {
		return fmt.Errorf("AWS_ACCESS_KEY_ID is missing. This should be set in " +
			"the config file as AwsAccessKeyId or in the environment as AWS_ACCESS_KEY_ID.")
	}
	if partnerConfig.AwsSecretAccessKey == "" {
		return fmt.Errorf("AWS_SECRET_ACCESS_KEY is missing. This should be set in " +
			"the config file as AwsSecretAccessKey or in the environment as AWS_SECRET_ACCESS_KEY.")
	}
	if partnerConfig.ReceivingBucket == "" {
		return fmt.Errorf("Config file setting ReceivingBucket is missing.")
	}
	if partnerConfig.RestorationBucket == "" {
		return fmt.Errorf("Config file setting ReceivingBucket is missing.")
	}
	if partnerConfig.DownloadDir == "" {
		return fmt.Errorf("Config file setting DownloadDir is missing.")
	} else {
		err := os.MkdirAll(partnerConfig.DownloadDir, 0755)
		if err != nil {
			return fmt.Errorf("Cannot created DownloadDir '%s': %v", partnerConfig.DownloadDir, err)
		}
	}
	return nil
}

func (partnerConfig *PartnerConfig) ExpandFilePaths() {
	expanded, err := fileutil.ExpandTilde(partnerConfig.DownloadDir)
	if err == nil {
		partnerConfig.DownloadDir = expanded
	}
}
