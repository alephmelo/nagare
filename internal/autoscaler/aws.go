package autoscaler

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/alephmelo/nagare/internal/config"
)

// AWSProvider implements CloudProvider using Amazon EC2.
//
// # Instance lifecycle
//
// SpinUp calls RunInstances with a minimal user-data script that:
//  1. Downloads the nagare binary from NagareDownloadURL (or expects it
//     pre-installed if an AMI with the binary is configured).
//  2. Runs: nagare --worker --join <MasterAddr> --pools <pools> --token <Token>
//
// All instances are tagged with:
//
//	nagare:managed-by = nagare-autoscaler
//	nagare:instance-id = <Nagare instance ID>
//	nagare:pools = <comma-separated pool names>
//
// SpinDown calls TerminateInstances on the EC2 instance ID.
// List calls DescribeInstances filtered by the nagare:managed-by tag.
//
// # Authentication
//
// The provider relies on the standard AWS SDK credential chain:
//  1. Environment variables (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)
//  2. ~/.aws/credentials
//  3. EC2 instance metadata (when running on AWS)
//
// No credentials are stored in nagare.yaml.
//
// # GPU instances
//
// When SpinUpRequest.NeedsGPU is true and AWSProviderConfig.GPUInstanceType
// is set, that instance type is selected instead of the default.
type AWSProvider struct {
	cfg config.AWSProviderConfig
	// ec2Client is the AWS EC2 API client.  It is an interface so tests can
	// inject a fake without real AWS credentials.
	ec2Client ec2Client
}

// ec2Client is the subset of the AWS EC2 API that AWSProvider uses.
// The full AWS SDK v2 implementation is in ec2RealClient (below).
type ec2Client interface {
	// RunInstances starts one EC2 instance and returns its instance ID.
	RunInstances(ctx context.Context, req ec2RunRequest) (string, error)
	// TerminateInstances stops and terminates one or more instances.
	TerminateInstances(ctx context.Context, instanceIDs []string) error
	// DescribeInstances lists instances matching the given tag filters.
	DescribeInstances(ctx context.Context, tagFilters map[string]string) ([]ec2InstanceInfo, error)
}

// ec2RunRequest carries the parameters for a RunInstances call.
type ec2RunRequest struct {
	ImageID            string
	InstanceType       string
	KeyName            string
	SecurityGroupID    string
	SubnetID           string
	IAMInstanceProfile string
	UserData           string // base64-encoded
	Tags               map[string]string
}

// ec2InstanceInfo is a minimal descriptor returned by DescribeInstances.
type ec2InstanceInfo struct {
	InstanceID string
	Tags       map[string]string
	State      string // "running" | "pending" | "terminated" | etc.
}

// NewAWSProvider creates an AWSProvider.
//
// This constructor validates the configuration and initialises the AWS SDK
// client.  It returns an error when required fields (Region, InstanceType,
// SecurityGroup, SubnetID) are missing, or when the SDK cannot be initialised.
//
// NOTE: The AWS SDK v2 is not yet a direct dependency of the project.  Until
// it is added to go.mod the real client will not compile; the interface allows
// the rest of the code to reference AWSProvider in tests via a fake.  Run
// `go get github.com/aws/aws-sdk-go-v2/...` to enable the real client.
func NewAWSProvider(cfg config.AWSProviderConfig) (*AWSProvider, error) {
	if cfg.Region == "" {
		return nil, fmt.Errorf("aws provider: region is required")
	}
	if cfg.InstanceType == "" {
		return nil, fmt.Errorf("aws provider: instance_type is required")
	}
	if cfg.SecurityGroup == "" {
		return nil, fmt.Errorf("aws provider: security_group is required")
	}
	if cfg.SubnetID == "" {
		return nil, fmt.Errorf("aws provider: subnet_id is required")
	}

	// TODO: initialise the real aws-sdk-go-v2 EC2 client once the dependency
	// has been added.  For now we return an error that makes the provider
	// unusable but allows the rest of the package to compile cleanly.
	//
	// To wire up the real client:
	//   cfg, _ := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion(cfg.Region))
	//   svc := ec2.NewFromConfig(cfg)
	//   return &AWSProvider{cfg: cfg, ec2Client: &ec2RealClient{svc: svc}}, nil
	return nil, fmt.Errorf("aws provider: real AWS SDK client not yet wired — add github.com/aws/aws-sdk-go-v2 to go.mod and implement ec2RealClient")
}

// newAWSProviderWithClient creates an AWSProvider with an injected ec2Client.
// Used exclusively in unit tests.
func newAWSProviderWithClient(cfg config.AWSProviderConfig, client ec2Client) *AWSProvider {
	return &AWSProvider{cfg: cfg, ec2Client: client}
}

// Name implements CloudProvider.
func (a *AWSProvider) Name() string { return "aws" }

// SpinUp implements CloudProvider.
//
// It calls RunInstances to create one EC2 instance, tagging it so that
// List() can find it on master restart.  The user-data script is generated
// from the configured NagareDownloadURL or relies on a pre-baked AMI.
func (a *AWSProvider) SpinUp(ctx context.Context, req SpinUpRequest) (WorkerInstance, error) {
	instanceType := a.cfg.InstanceType
	if req.NeedsGPU && a.cfg.GPUInstanceType != "" {
		instanceType = a.cfg.GPUInstanceType
	}

	userData := a.buildUserData(req)

	tags := map[string]string{
		"nagare:managed-by":  "nagare-autoscaler",
		"nagare:instance-id": req.InstanceID,
		"nagare:pools":       strings.Join(req.Pools, ","),
		"Name":               fmt.Sprintf("nagare-worker-%s", req.InstanceID),
	}

	runReq := ec2RunRequest{
		ImageID:            a.cfg.AMIID,
		InstanceType:       instanceType,
		KeyName:            a.cfg.KeyName,
		SecurityGroupID:    a.cfg.SecurityGroup,
		SubnetID:           a.cfg.SubnetID,
		IAMInstanceProfile: a.cfg.IAMInstanceProfile,
		UserData:           base64.StdEncoding.EncodeToString([]byte(userData)),
		Tags:               tags,
	}

	ec2ID, err := a.ec2Client.RunInstances(ctx, runReq)
	if err != nil {
		return WorkerInstance{}, fmt.Errorf("aws provider: SpinUp %s: %w", req.InstanceID, err)
	}

	return WorkerInstance{
		ID:           req.InstanceID,
		ProviderID:   ec2ID,
		Pools:        req.Pools,
		InstanceType: instanceType,
		Region:       a.cfg.Region,
		Status:       InstanceProvisioning,
		CreatedAt:    time.Now(),
	}, nil
}

// SpinDown implements CloudProvider.
//
// It calls TerminateInstances on the given EC2 instance ID.
func (a *AWSProvider) SpinDown(ctx context.Context, providerID string) error {
	if err := a.ec2Client.TerminateInstances(ctx, []string{providerID}); err != nil {
		return fmt.Errorf("aws provider: SpinDown %s: %w", providerID, err)
	}
	return nil
}

// List implements CloudProvider.
//
// It calls DescribeInstances filtered by the nagare:managed-by tag so only
// Nagare-managed instances are returned.
func (a *AWSProvider) List(ctx context.Context) ([]WorkerInstance, error) {
	infos, err := a.ec2Client.DescribeInstances(ctx, map[string]string{
		"nagare:managed-by": "nagare-autoscaler",
	})
	if err != nil {
		return nil, fmt.Errorf("aws provider: List: %w", err)
	}

	out := make([]WorkerInstance, 0, len(infos))
	for _, info := range infos {
		if info.State == "terminated" || info.State == "shutting-down" {
			continue
		}
		pools := strings.Split(info.Tags["nagare:pools"], ",")
		out = append(out, WorkerInstance{
			ID:         info.Tags["nagare:instance-id"],
			ProviderID: info.InstanceID,
			Pools:      pools,
			Region:     a.cfg.Region,
			Status:     InstanceProvisioning, // reconciled by the autoscaler
		})
	}
	return out, nil
}

// buildUserData generates the EC2 user-data shell script that starts the nagare
// worker process on the instance.
//
// When NagareDownloadURL is configured, the script downloads the binary at
// boot time.  When an AMI with a pre-installed binary is used, the download
// step is skipped (the binary is expected at /usr/local/bin/nagare).
func (a *AWSProvider) buildUserData(req SpinUpRequest) string {
	var sb strings.Builder
	sb.WriteString("#!/bin/bash\nset -euo pipefail\n")

	if a.cfg.NagareDownloadURL != "" {
		fmt.Fprintf(&sb, "curl -fsSL %q -o /usr/local/bin/nagare\n", a.cfg.NagareDownloadURL)
		sb.WriteString("chmod +x /usr/local/bin/nagare\n")
	}

	// Build the worker command.
	cmd := fmt.Sprintf(
		"/usr/local/bin/nagare --worker --join %q --pools %q",
		req.MasterAddr,
		strings.Join(req.Pools, ","),
	)
	if req.Token != "" {
		cmd += fmt.Sprintf(" --token %q", req.Token)
	}

	// Run in background, redirect output to syslog.
	fmt.Fprintf(&sb, "nohup %s >> /var/log/nagare-worker.log 2>&1 &\n", cmd)
	return sb.String()
}
