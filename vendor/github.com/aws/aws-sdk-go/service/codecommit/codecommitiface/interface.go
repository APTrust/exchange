// Code generated by private/model/cli/gen-api/main.go. DO NOT EDIT.

// Package codecommitiface provides an interface to enable mocking the AWS CodeCommit service client
// for testing your code.
//
// It is important to note that this interface will have breaking changes
// when the service model is updated and adds new API operations, paginators,
// and waiters.
package codecommitiface

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/codecommit"
)

// CodeCommitAPI provides an interface to enable mocking the
// codecommit.CodeCommit service client's API operation,
// paginators, and waiters. This make unit testing your code that calls out
// to the SDK's service client's calls easier.
//
// The best way to use this interface is so the SDK's service client's calls
// can be stubbed out for unit testing your code with the SDK without needing
// to inject custom request handlers into the SDK's request pipeline.
//
//    // myFunc uses an SDK service client to make a request to
//    // AWS CodeCommit.
//    func myFunc(svc codecommitiface.CodeCommitAPI) bool {
//        // Make svc.BatchDescribeMergeConflicts request
//    }
//
//    func main() {
//        sess := session.New()
//        svc := codecommit.New(sess)
//
//        myFunc(svc)
//    }
//
// In your _test.go file:
//
//    // Define a mock struct to be used in your unit tests of myFunc.
//    type mockCodeCommitClient struct {
//        codecommitiface.CodeCommitAPI
//    }
//    func (m *mockCodeCommitClient) BatchDescribeMergeConflicts(input *codecommit.BatchDescribeMergeConflictsInput) (*codecommit.BatchDescribeMergeConflictsOutput, error) {
//        // mock response/functionality
//    }
//
//    func TestMyFunc(t *testing.T) {
//        // Setup Test
//        mockSvc := &mockCodeCommitClient{}
//
//        myfunc(mockSvc)
//
//        // Verify myFunc's functionality
//    }
//
// It is important to note that this interface will have breaking changes
// when the service model is updated and adds new API operations, paginators,
// and waiters. Its suggested to use the pattern above for testing, or using
// tooling to generate mocks to satisfy the interfaces.
type CodeCommitAPI interface {
	BatchDescribeMergeConflicts(*codecommit.BatchDescribeMergeConflictsInput) (*codecommit.BatchDescribeMergeConflictsOutput, error)
	BatchDescribeMergeConflictsWithContext(aws.Context, *codecommit.BatchDescribeMergeConflictsInput, ...request.Option) (*codecommit.BatchDescribeMergeConflictsOutput, error)
	BatchDescribeMergeConflictsRequest(*codecommit.BatchDescribeMergeConflictsInput) (*request.Request, *codecommit.BatchDescribeMergeConflictsOutput)

	BatchGetCommits(*codecommit.BatchGetCommitsInput) (*codecommit.BatchGetCommitsOutput, error)
	BatchGetCommitsWithContext(aws.Context, *codecommit.BatchGetCommitsInput, ...request.Option) (*codecommit.BatchGetCommitsOutput, error)
	BatchGetCommitsRequest(*codecommit.BatchGetCommitsInput) (*request.Request, *codecommit.BatchGetCommitsOutput)

	BatchGetRepositories(*codecommit.BatchGetRepositoriesInput) (*codecommit.BatchGetRepositoriesOutput, error)
	BatchGetRepositoriesWithContext(aws.Context, *codecommit.BatchGetRepositoriesInput, ...request.Option) (*codecommit.BatchGetRepositoriesOutput, error)
	BatchGetRepositoriesRequest(*codecommit.BatchGetRepositoriesInput) (*request.Request, *codecommit.BatchGetRepositoriesOutput)

	CreateBranch(*codecommit.CreateBranchInput) (*codecommit.CreateBranchOutput, error)
	CreateBranchWithContext(aws.Context, *codecommit.CreateBranchInput, ...request.Option) (*codecommit.CreateBranchOutput, error)
	CreateBranchRequest(*codecommit.CreateBranchInput) (*request.Request, *codecommit.CreateBranchOutput)

	CreateCommit(*codecommit.CreateCommitInput) (*codecommit.CreateCommitOutput, error)
	CreateCommitWithContext(aws.Context, *codecommit.CreateCommitInput, ...request.Option) (*codecommit.CreateCommitOutput, error)
	CreateCommitRequest(*codecommit.CreateCommitInput) (*request.Request, *codecommit.CreateCommitOutput)

	CreatePullRequest(*codecommit.CreatePullRequestInput) (*codecommit.CreatePullRequestOutput, error)
	CreatePullRequestWithContext(aws.Context, *codecommit.CreatePullRequestInput, ...request.Option) (*codecommit.CreatePullRequestOutput, error)
	CreatePullRequestRequest(*codecommit.CreatePullRequestInput) (*request.Request, *codecommit.CreatePullRequestOutput)

	CreateRepository(*codecommit.CreateRepositoryInput) (*codecommit.CreateRepositoryOutput, error)
	CreateRepositoryWithContext(aws.Context, *codecommit.CreateRepositoryInput, ...request.Option) (*codecommit.CreateRepositoryOutput, error)
	CreateRepositoryRequest(*codecommit.CreateRepositoryInput) (*request.Request, *codecommit.CreateRepositoryOutput)

	CreateUnreferencedMergeCommit(*codecommit.CreateUnreferencedMergeCommitInput) (*codecommit.CreateUnreferencedMergeCommitOutput, error)
	CreateUnreferencedMergeCommitWithContext(aws.Context, *codecommit.CreateUnreferencedMergeCommitInput, ...request.Option) (*codecommit.CreateUnreferencedMergeCommitOutput, error)
	CreateUnreferencedMergeCommitRequest(*codecommit.CreateUnreferencedMergeCommitInput) (*request.Request, *codecommit.CreateUnreferencedMergeCommitOutput)

	DeleteBranch(*codecommit.DeleteBranchInput) (*codecommit.DeleteBranchOutput, error)
	DeleteBranchWithContext(aws.Context, *codecommit.DeleteBranchInput, ...request.Option) (*codecommit.DeleteBranchOutput, error)
	DeleteBranchRequest(*codecommit.DeleteBranchInput) (*request.Request, *codecommit.DeleteBranchOutput)

	DeleteCommentContent(*codecommit.DeleteCommentContentInput) (*codecommit.DeleteCommentContentOutput, error)
	DeleteCommentContentWithContext(aws.Context, *codecommit.DeleteCommentContentInput, ...request.Option) (*codecommit.DeleteCommentContentOutput, error)
	DeleteCommentContentRequest(*codecommit.DeleteCommentContentInput) (*request.Request, *codecommit.DeleteCommentContentOutput)

	DeleteFile(*codecommit.DeleteFileInput) (*codecommit.DeleteFileOutput, error)
	DeleteFileWithContext(aws.Context, *codecommit.DeleteFileInput, ...request.Option) (*codecommit.DeleteFileOutput, error)
	DeleteFileRequest(*codecommit.DeleteFileInput) (*request.Request, *codecommit.DeleteFileOutput)

	DeleteRepository(*codecommit.DeleteRepositoryInput) (*codecommit.DeleteRepositoryOutput, error)
	DeleteRepositoryWithContext(aws.Context, *codecommit.DeleteRepositoryInput, ...request.Option) (*codecommit.DeleteRepositoryOutput, error)
	DeleteRepositoryRequest(*codecommit.DeleteRepositoryInput) (*request.Request, *codecommit.DeleteRepositoryOutput)

	DescribeMergeConflicts(*codecommit.DescribeMergeConflictsInput) (*codecommit.DescribeMergeConflictsOutput, error)
	DescribeMergeConflictsWithContext(aws.Context, *codecommit.DescribeMergeConflictsInput, ...request.Option) (*codecommit.DescribeMergeConflictsOutput, error)
	DescribeMergeConflictsRequest(*codecommit.DescribeMergeConflictsInput) (*request.Request, *codecommit.DescribeMergeConflictsOutput)

	DescribeMergeConflictsPages(*codecommit.DescribeMergeConflictsInput, func(*codecommit.DescribeMergeConflictsOutput, bool) bool) error
	DescribeMergeConflictsPagesWithContext(aws.Context, *codecommit.DescribeMergeConflictsInput, func(*codecommit.DescribeMergeConflictsOutput, bool) bool, ...request.Option) error

	DescribePullRequestEvents(*codecommit.DescribePullRequestEventsInput) (*codecommit.DescribePullRequestEventsOutput, error)
	DescribePullRequestEventsWithContext(aws.Context, *codecommit.DescribePullRequestEventsInput, ...request.Option) (*codecommit.DescribePullRequestEventsOutput, error)
	DescribePullRequestEventsRequest(*codecommit.DescribePullRequestEventsInput) (*request.Request, *codecommit.DescribePullRequestEventsOutput)

	DescribePullRequestEventsPages(*codecommit.DescribePullRequestEventsInput, func(*codecommit.DescribePullRequestEventsOutput, bool) bool) error
	DescribePullRequestEventsPagesWithContext(aws.Context, *codecommit.DescribePullRequestEventsInput, func(*codecommit.DescribePullRequestEventsOutput, bool) bool, ...request.Option) error

	GetBlob(*codecommit.GetBlobInput) (*codecommit.GetBlobOutput, error)
	GetBlobWithContext(aws.Context, *codecommit.GetBlobInput, ...request.Option) (*codecommit.GetBlobOutput, error)
	GetBlobRequest(*codecommit.GetBlobInput) (*request.Request, *codecommit.GetBlobOutput)

	GetBranch(*codecommit.GetBranchInput) (*codecommit.GetBranchOutput, error)
	GetBranchWithContext(aws.Context, *codecommit.GetBranchInput, ...request.Option) (*codecommit.GetBranchOutput, error)
	GetBranchRequest(*codecommit.GetBranchInput) (*request.Request, *codecommit.GetBranchOutput)

	GetComment(*codecommit.GetCommentInput) (*codecommit.GetCommentOutput, error)
	GetCommentWithContext(aws.Context, *codecommit.GetCommentInput, ...request.Option) (*codecommit.GetCommentOutput, error)
	GetCommentRequest(*codecommit.GetCommentInput) (*request.Request, *codecommit.GetCommentOutput)

	GetCommentsForComparedCommit(*codecommit.GetCommentsForComparedCommitInput) (*codecommit.GetCommentsForComparedCommitOutput, error)
	GetCommentsForComparedCommitWithContext(aws.Context, *codecommit.GetCommentsForComparedCommitInput, ...request.Option) (*codecommit.GetCommentsForComparedCommitOutput, error)
	GetCommentsForComparedCommitRequest(*codecommit.GetCommentsForComparedCommitInput) (*request.Request, *codecommit.GetCommentsForComparedCommitOutput)

	GetCommentsForComparedCommitPages(*codecommit.GetCommentsForComparedCommitInput, func(*codecommit.GetCommentsForComparedCommitOutput, bool) bool) error
	GetCommentsForComparedCommitPagesWithContext(aws.Context, *codecommit.GetCommentsForComparedCommitInput, func(*codecommit.GetCommentsForComparedCommitOutput, bool) bool, ...request.Option) error

	GetCommentsForPullRequest(*codecommit.GetCommentsForPullRequestInput) (*codecommit.GetCommentsForPullRequestOutput, error)
	GetCommentsForPullRequestWithContext(aws.Context, *codecommit.GetCommentsForPullRequestInput, ...request.Option) (*codecommit.GetCommentsForPullRequestOutput, error)
	GetCommentsForPullRequestRequest(*codecommit.GetCommentsForPullRequestInput) (*request.Request, *codecommit.GetCommentsForPullRequestOutput)

	GetCommentsForPullRequestPages(*codecommit.GetCommentsForPullRequestInput, func(*codecommit.GetCommentsForPullRequestOutput, bool) bool) error
	GetCommentsForPullRequestPagesWithContext(aws.Context, *codecommit.GetCommentsForPullRequestInput, func(*codecommit.GetCommentsForPullRequestOutput, bool) bool, ...request.Option) error

	GetCommit(*codecommit.GetCommitInput) (*codecommit.GetCommitOutput, error)
	GetCommitWithContext(aws.Context, *codecommit.GetCommitInput, ...request.Option) (*codecommit.GetCommitOutput, error)
	GetCommitRequest(*codecommit.GetCommitInput) (*request.Request, *codecommit.GetCommitOutput)

	GetDifferences(*codecommit.GetDifferencesInput) (*codecommit.GetDifferencesOutput, error)
	GetDifferencesWithContext(aws.Context, *codecommit.GetDifferencesInput, ...request.Option) (*codecommit.GetDifferencesOutput, error)
	GetDifferencesRequest(*codecommit.GetDifferencesInput) (*request.Request, *codecommit.GetDifferencesOutput)

	GetDifferencesPages(*codecommit.GetDifferencesInput, func(*codecommit.GetDifferencesOutput, bool) bool) error
	GetDifferencesPagesWithContext(aws.Context, *codecommit.GetDifferencesInput, func(*codecommit.GetDifferencesOutput, bool) bool, ...request.Option) error

	GetFile(*codecommit.GetFileInput) (*codecommit.GetFileOutput, error)
	GetFileWithContext(aws.Context, *codecommit.GetFileInput, ...request.Option) (*codecommit.GetFileOutput, error)
	GetFileRequest(*codecommit.GetFileInput) (*request.Request, *codecommit.GetFileOutput)

	GetFolder(*codecommit.GetFolderInput) (*codecommit.GetFolderOutput, error)
	GetFolderWithContext(aws.Context, *codecommit.GetFolderInput, ...request.Option) (*codecommit.GetFolderOutput, error)
	GetFolderRequest(*codecommit.GetFolderInput) (*request.Request, *codecommit.GetFolderOutput)

	GetMergeCommit(*codecommit.GetMergeCommitInput) (*codecommit.GetMergeCommitOutput, error)
	GetMergeCommitWithContext(aws.Context, *codecommit.GetMergeCommitInput, ...request.Option) (*codecommit.GetMergeCommitOutput, error)
	GetMergeCommitRequest(*codecommit.GetMergeCommitInput) (*request.Request, *codecommit.GetMergeCommitOutput)

	GetMergeConflicts(*codecommit.GetMergeConflictsInput) (*codecommit.GetMergeConflictsOutput, error)
	GetMergeConflictsWithContext(aws.Context, *codecommit.GetMergeConflictsInput, ...request.Option) (*codecommit.GetMergeConflictsOutput, error)
	GetMergeConflictsRequest(*codecommit.GetMergeConflictsInput) (*request.Request, *codecommit.GetMergeConflictsOutput)

	GetMergeConflictsPages(*codecommit.GetMergeConflictsInput, func(*codecommit.GetMergeConflictsOutput, bool) bool) error
	GetMergeConflictsPagesWithContext(aws.Context, *codecommit.GetMergeConflictsInput, func(*codecommit.GetMergeConflictsOutput, bool) bool, ...request.Option) error

	GetMergeOptions(*codecommit.GetMergeOptionsInput) (*codecommit.GetMergeOptionsOutput, error)
	GetMergeOptionsWithContext(aws.Context, *codecommit.GetMergeOptionsInput, ...request.Option) (*codecommit.GetMergeOptionsOutput, error)
	GetMergeOptionsRequest(*codecommit.GetMergeOptionsInput) (*request.Request, *codecommit.GetMergeOptionsOutput)

	GetPullRequest(*codecommit.GetPullRequestInput) (*codecommit.GetPullRequestOutput, error)
	GetPullRequestWithContext(aws.Context, *codecommit.GetPullRequestInput, ...request.Option) (*codecommit.GetPullRequestOutput, error)
	GetPullRequestRequest(*codecommit.GetPullRequestInput) (*request.Request, *codecommit.GetPullRequestOutput)

	GetRepository(*codecommit.GetRepositoryInput) (*codecommit.GetRepositoryOutput, error)
	GetRepositoryWithContext(aws.Context, *codecommit.GetRepositoryInput, ...request.Option) (*codecommit.GetRepositoryOutput, error)
	GetRepositoryRequest(*codecommit.GetRepositoryInput) (*request.Request, *codecommit.GetRepositoryOutput)

	GetRepositoryTriggers(*codecommit.GetRepositoryTriggersInput) (*codecommit.GetRepositoryTriggersOutput, error)
	GetRepositoryTriggersWithContext(aws.Context, *codecommit.GetRepositoryTriggersInput, ...request.Option) (*codecommit.GetRepositoryTriggersOutput, error)
	GetRepositoryTriggersRequest(*codecommit.GetRepositoryTriggersInput) (*request.Request, *codecommit.GetRepositoryTriggersOutput)

	ListBranches(*codecommit.ListBranchesInput) (*codecommit.ListBranchesOutput, error)
	ListBranchesWithContext(aws.Context, *codecommit.ListBranchesInput, ...request.Option) (*codecommit.ListBranchesOutput, error)
	ListBranchesRequest(*codecommit.ListBranchesInput) (*request.Request, *codecommit.ListBranchesOutput)

	ListBranchesPages(*codecommit.ListBranchesInput, func(*codecommit.ListBranchesOutput, bool) bool) error
	ListBranchesPagesWithContext(aws.Context, *codecommit.ListBranchesInput, func(*codecommit.ListBranchesOutput, bool) bool, ...request.Option) error

	ListPullRequests(*codecommit.ListPullRequestsInput) (*codecommit.ListPullRequestsOutput, error)
	ListPullRequestsWithContext(aws.Context, *codecommit.ListPullRequestsInput, ...request.Option) (*codecommit.ListPullRequestsOutput, error)
	ListPullRequestsRequest(*codecommit.ListPullRequestsInput) (*request.Request, *codecommit.ListPullRequestsOutput)

	ListPullRequestsPages(*codecommit.ListPullRequestsInput, func(*codecommit.ListPullRequestsOutput, bool) bool) error
	ListPullRequestsPagesWithContext(aws.Context, *codecommit.ListPullRequestsInput, func(*codecommit.ListPullRequestsOutput, bool) bool, ...request.Option) error

	ListRepositories(*codecommit.ListRepositoriesInput) (*codecommit.ListRepositoriesOutput, error)
	ListRepositoriesWithContext(aws.Context, *codecommit.ListRepositoriesInput, ...request.Option) (*codecommit.ListRepositoriesOutput, error)
	ListRepositoriesRequest(*codecommit.ListRepositoriesInput) (*request.Request, *codecommit.ListRepositoriesOutput)

	ListRepositoriesPages(*codecommit.ListRepositoriesInput, func(*codecommit.ListRepositoriesOutput, bool) bool) error
	ListRepositoriesPagesWithContext(aws.Context, *codecommit.ListRepositoriesInput, func(*codecommit.ListRepositoriesOutput, bool) bool, ...request.Option) error

	ListTagsForResource(*codecommit.ListTagsForResourceInput) (*codecommit.ListTagsForResourceOutput, error)
	ListTagsForResourceWithContext(aws.Context, *codecommit.ListTagsForResourceInput, ...request.Option) (*codecommit.ListTagsForResourceOutput, error)
	ListTagsForResourceRequest(*codecommit.ListTagsForResourceInput) (*request.Request, *codecommit.ListTagsForResourceOutput)

	MergeBranchesByFastForward(*codecommit.MergeBranchesByFastForwardInput) (*codecommit.MergeBranchesByFastForwardOutput, error)
	MergeBranchesByFastForwardWithContext(aws.Context, *codecommit.MergeBranchesByFastForwardInput, ...request.Option) (*codecommit.MergeBranchesByFastForwardOutput, error)
	MergeBranchesByFastForwardRequest(*codecommit.MergeBranchesByFastForwardInput) (*request.Request, *codecommit.MergeBranchesByFastForwardOutput)

	MergeBranchesBySquash(*codecommit.MergeBranchesBySquashInput) (*codecommit.MergeBranchesBySquashOutput, error)
	MergeBranchesBySquashWithContext(aws.Context, *codecommit.MergeBranchesBySquashInput, ...request.Option) (*codecommit.MergeBranchesBySquashOutput, error)
	MergeBranchesBySquashRequest(*codecommit.MergeBranchesBySquashInput) (*request.Request, *codecommit.MergeBranchesBySquashOutput)

	MergeBranchesByThreeWay(*codecommit.MergeBranchesByThreeWayInput) (*codecommit.MergeBranchesByThreeWayOutput, error)
	MergeBranchesByThreeWayWithContext(aws.Context, *codecommit.MergeBranchesByThreeWayInput, ...request.Option) (*codecommit.MergeBranchesByThreeWayOutput, error)
	MergeBranchesByThreeWayRequest(*codecommit.MergeBranchesByThreeWayInput) (*request.Request, *codecommit.MergeBranchesByThreeWayOutput)

	MergePullRequestByFastForward(*codecommit.MergePullRequestByFastForwardInput) (*codecommit.MergePullRequestByFastForwardOutput, error)
	MergePullRequestByFastForwardWithContext(aws.Context, *codecommit.MergePullRequestByFastForwardInput, ...request.Option) (*codecommit.MergePullRequestByFastForwardOutput, error)
	MergePullRequestByFastForwardRequest(*codecommit.MergePullRequestByFastForwardInput) (*request.Request, *codecommit.MergePullRequestByFastForwardOutput)

	MergePullRequestBySquash(*codecommit.MergePullRequestBySquashInput) (*codecommit.MergePullRequestBySquashOutput, error)
	MergePullRequestBySquashWithContext(aws.Context, *codecommit.MergePullRequestBySquashInput, ...request.Option) (*codecommit.MergePullRequestBySquashOutput, error)
	MergePullRequestBySquashRequest(*codecommit.MergePullRequestBySquashInput) (*request.Request, *codecommit.MergePullRequestBySquashOutput)

	MergePullRequestByThreeWay(*codecommit.MergePullRequestByThreeWayInput) (*codecommit.MergePullRequestByThreeWayOutput, error)
	MergePullRequestByThreeWayWithContext(aws.Context, *codecommit.MergePullRequestByThreeWayInput, ...request.Option) (*codecommit.MergePullRequestByThreeWayOutput, error)
	MergePullRequestByThreeWayRequest(*codecommit.MergePullRequestByThreeWayInput) (*request.Request, *codecommit.MergePullRequestByThreeWayOutput)

	PostCommentForComparedCommit(*codecommit.PostCommentForComparedCommitInput) (*codecommit.PostCommentForComparedCommitOutput, error)
	PostCommentForComparedCommitWithContext(aws.Context, *codecommit.PostCommentForComparedCommitInput, ...request.Option) (*codecommit.PostCommentForComparedCommitOutput, error)
	PostCommentForComparedCommitRequest(*codecommit.PostCommentForComparedCommitInput) (*request.Request, *codecommit.PostCommentForComparedCommitOutput)

	PostCommentForPullRequest(*codecommit.PostCommentForPullRequestInput) (*codecommit.PostCommentForPullRequestOutput, error)
	PostCommentForPullRequestWithContext(aws.Context, *codecommit.PostCommentForPullRequestInput, ...request.Option) (*codecommit.PostCommentForPullRequestOutput, error)
	PostCommentForPullRequestRequest(*codecommit.PostCommentForPullRequestInput) (*request.Request, *codecommit.PostCommentForPullRequestOutput)

	PostCommentReply(*codecommit.PostCommentReplyInput) (*codecommit.PostCommentReplyOutput, error)
	PostCommentReplyWithContext(aws.Context, *codecommit.PostCommentReplyInput, ...request.Option) (*codecommit.PostCommentReplyOutput, error)
	PostCommentReplyRequest(*codecommit.PostCommentReplyInput) (*request.Request, *codecommit.PostCommentReplyOutput)

	PutFile(*codecommit.PutFileInput) (*codecommit.PutFileOutput, error)
	PutFileWithContext(aws.Context, *codecommit.PutFileInput, ...request.Option) (*codecommit.PutFileOutput, error)
	PutFileRequest(*codecommit.PutFileInput) (*request.Request, *codecommit.PutFileOutput)

	PutRepositoryTriggers(*codecommit.PutRepositoryTriggersInput) (*codecommit.PutRepositoryTriggersOutput, error)
	PutRepositoryTriggersWithContext(aws.Context, *codecommit.PutRepositoryTriggersInput, ...request.Option) (*codecommit.PutRepositoryTriggersOutput, error)
	PutRepositoryTriggersRequest(*codecommit.PutRepositoryTriggersInput) (*request.Request, *codecommit.PutRepositoryTriggersOutput)

	TagResource(*codecommit.TagResourceInput) (*codecommit.TagResourceOutput, error)
	TagResourceWithContext(aws.Context, *codecommit.TagResourceInput, ...request.Option) (*codecommit.TagResourceOutput, error)
	TagResourceRequest(*codecommit.TagResourceInput) (*request.Request, *codecommit.TagResourceOutput)

	TestRepositoryTriggers(*codecommit.TestRepositoryTriggersInput) (*codecommit.TestRepositoryTriggersOutput, error)
	TestRepositoryTriggersWithContext(aws.Context, *codecommit.TestRepositoryTriggersInput, ...request.Option) (*codecommit.TestRepositoryTriggersOutput, error)
	TestRepositoryTriggersRequest(*codecommit.TestRepositoryTriggersInput) (*request.Request, *codecommit.TestRepositoryTriggersOutput)

	UntagResource(*codecommit.UntagResourceInput) (*codecommit.UntagResourceOutput, error)
	UntagResourceWithContext(aws.Context, *codecommit.UntagResourceInput, ...request.Option) (*codecommit.UntagResourceOutput, error)
	UntagResourceRequest(*codecommit.UntagResourceInput) (*request.Request, *codecommit.UntagResourceOutput)

	UpdateComment(*codecommit.UpdateCommentInput) (*codecommit.UpdateCommentOutput, error)
	UpdateCommentWithContext(aws.Context, *codecommit.UpdateCommentInput, ...request.Option) (*codecommit.UpdateCommentOutput, error)
	UpdateCommentRequest(*codecommit.UpdateCommentInput) (*request.Request, *codecommit.UpdateCommentOutput)

	UpdateDefaultBranch(*codecommit.UpdateDefaultBranchInput) (*codecommit.UpdateDefaultBranchOutput, error)
	UpdateDefaultBranchWithContext(aws.Context, *codecommit.UpdateDefaultBranchInput, ...request.Option) (*codecommit.UpdateDefaultBranchOutput, error)
	UpdateDefaultBranchRequest(*codecommit.UpdateDefaultBranchInput) (*request.Request, *codecommit.UpdateDefaultBranchOutput)

	UpdatePullRequestDescription(*codecommit.UpdatePullRequestDescriptionInput) (*codecommit.UpdatePullRequestDescriptionOutput, error)
	UpdatePullRequestDescriptionWithContext(aws.Context, *codecommit.UpdatePullRequestDescriptionInput, ...request.Option) (*codecommit.UpdatePullRequestDescriptionOutput, error)
	UpdatePullRequestDescriptionRequest(*codecommit.UpdatePullRequestDescriptionInput) (*request.Request, *codecommit.UpdatePullRequestDescriptionOutput)

	UpdatePullRequestStatus(*codecommit.UpdatePullRequestStatusInput) (*codecommit.UpdatePullRequestStatusOutput, error)
	UpdatePullRequestStatusWithContext(aws.Context, *codecommit.UpdatePullRequestStatusInput, ...request.Option) (*codecommit.UpdatePullRequestStatusOutput, error)
	UpdatePullRequestStatusRequest(*codecommit.UpdatePullRequestStatusInput) (*request.Request, *codecommit.UpdatePullRequestStatusOutput)

	UpdatePullRequestTitle(*codecommit.UpdatePullRequestTitleInput) (*codecommit.UpdatePullRequestTitleOutput, error)
	UpdatePullRequestTitleWithContext(aws.Context, *codecommit.UpdatePullRequestTitleInput, ...request.Option) (*codecommit.UpdatePullRequestTitleOutput, error)
	UpdatePullRequestTitleRequest(*codecommit.UpdatePullRequestTitleInput) (*request.Request, *codecommit.UpdatePullRequestTitleOutput)

	UpdateRepositoryDescription(*codecommit.UpdateRepositoryDescriptionInput) (*codecommit.UpdateRepositoryDescriptionOutput, error)
	UpdateRepositoryDescriptionWithContext(aws.Context, *codecommit.UpdateRepositoryDescriptionInput, ...request.Option) (*codecommit.UpdateRepositoryDescriptionOutput, error)
	UpdateRepositoryDescriptionRequest(*codecommit.UpdateRepositoryDescriptionInput) (*request.Request, *codecommit.UpdateRepositoryDescriptionOutput)

	UpdateRepositoryName(*codecommit.UpdateRepositoryNameInput) (*codecommit.UpdateRepositoryNameOutput, error)
	UpdateRepositoryNameWithContext(aws.Context, *codecommit.UpdateRepositoryNameInput, ...request.Option) (*codecommit.UpdateRepositoryNameOutput, error)
	UpdateRepositoryNameRequest(*codecommit.UpdateRepositoryNameInput) (*request.Request, *codecommit.UpdateRepositoryNameOutput)
}

var _ CodeCommitAPI = (*codecommit.CodeCommit)(nil)
