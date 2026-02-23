package autoscaler

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"

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

// NewAWSProvider creates an AWSProvider backed by the real AWS SDK v2 EC2
// client.
//
// It validates the required configuration fields (Region, InstanceType,
// SecurityGroup, SubnetID) and initialises the SDK using the following
// credential resolution order:
//
//  1. If cfg.Profile is set, the named profile from ~/.aws/config is used.
//  2. Otherwise the standard AWS SDK credential chain applies:
//     environment variables (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY) →
//     default profile in ~/.aws/credentials → EC2 instance metadata (IMDS).
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

	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
	}
	if cfg.Profile != "" {
		opts = append(opts, awsconfig.WithSharedConfigProfile(cfg.Profile))
	}

	sdkCfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("aws provider: load SDK config: %w", err)
	}

	svc := ec2.NewFromConfig(sdkCfg)
	return &AWSProvider{cfg: cfg, ec2Client: &ec2RealClient{svc: svc}}, nil
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

// ── Real AWS SDK v2 client ────────────────────────────────────────────────────

// ec2RealClient implements ec2Client using the AWS SDK v2 EC2 service.
type ec2RealClient struct {
	svc *ec2.Client
}

// RunInstances starts one EC2 instance and returns its EC2 instance ID.
func (c *ec2RealClient) RunInstances(ctx context.Context, req ec2RunRequest) (string, error) {
	input := &ec2.RunInstancesInput{
		MinCount:         aws.Int32(1),
		MaxCount:         aws.Int32(1),
		ImageId:          aws.String(req.ImageID),
		InstanceType:     ec2types.InstanceType(req.InstanceType),
		SubnetId:         aws.String(req.SubnetID),
		UserData:         aws.String(req.UserData),
		SecurityGroupIds: []string{req.SecurityGroupID},
	}
	if req.KeyName != "" {
		input.KeyName = aws.String(req.KeyName)
	}
	if req.IAMInstanceProfile != "" {
		input.IamInstanceProfile = &ec2types.IamInstanceProfileSpecification{
			Name: aws.String(req.IAMInstanceProfile),
		}
	}
	if len(req.Tags) > 0 {
		tags := make([]ec2types.Tag, 0, len(req.Tags))
		for k, v := range req.Tags {
			tags = append(tags, ec2types.Tag{Key: aws.String(k), Value: aws.String(v)})
		}
		input.TagSpecifications = []ec2types.TagSpecification{
			{ResourceType: ec2types.ResourceTypeInstance, Tags: tags},
		}
	}

	out, err := c.svc.RunInstances(ctx, input)
	if err != nil {
		return "", fmt.Errorf("RunInstances: %w", err)
	}
	if len(out.Instances) == 0 {
		return "", fmt.Errorf("RunInstances: no instances returned")
	}
	return aws.ToString(out.Instances[0].InstanceId), nil
}

// TerminateInstances terminates the given EC2 instances.
func (c *ec2RealClient) TerminateInstances(ctx context.Context, instanceIDs []string) error {
	_, err := c.svc.TerminateInstances(ctx, &ec2.TerminateInstancesInput{
		InstanceIds: instanceIDs,
	})
	if err != nil {
		return fmt.Errorf("TerminateInstances: %w", err)
	}
	return nil
}

// DescribeInstances lists instances matching the given tag filters.
func (c *ec2RealClient) DescribeInstances(ctx context.Context, tagFilters map[string]string) ([]ec2InstanceInfo, error) {
	filters := make([]ec2types.Filter, 0, len(tagFilters))
	for k, v := range tagFilters {
		filters = append(filters, ec2types.Filter{
			Name:   aws.String("tag:" + k),
			Values: []string{v},
		})
	}

	out, err := c.svc.DescribeInstances(ctx, &ec2.DescribeInstancesInput{
		Filters: filters,
	})
	if err != nil {
		return nil, fmt.Errorf("DescribeInstances: %w", err)
	}

	var result []ec2InstanceInfo
	for _, reservation := range out.Reservations {
		for _, inst := range reservation.Instances {
			tags := make(map[string]string, len(inst.Tags))
			for _, t := range inst.Tags {
				tags[aws.ToString(t.Key)] = aws.ToString(t.Value)
			}
			result = append(result, ec2InstanceInfo{
				InstanceID: aws.ToString(inst.InstanceId),
				Tags:       tags,
				State:      string(inst.State.Name),
			})
		}
	}
	return result, nil
}
