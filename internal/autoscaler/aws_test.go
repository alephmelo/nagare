package autoscaler

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/alephmelo/nagare/internal/config"
)

// ---- Fake EC2 client ----

type fakeEC2Client struct {
	instances    map[string]ec2InstanceInfo
	runErr       error
	terminateErr error
	describeErr  error
}

func newFakeEC2Client() *fakeEC2Client {
	return &fakeEC2Client{instances: make(map[string]ec2InstanceInfo)}
}

func (f *fakeEC2Client) RunInstances(_ context.Context, req ec2RunRequest) (string, error) {
	if f.runErr != nil {
		return "", f.runErr
	}
	id := fmt.Sprintf("i-%08x", len(f.instances))
	f.instances[id] = ec2InstanceInfo{
		InstanceID: id,
		Tags:       req.Tags,
		State:      "running",
	}
	return id, nil
}

func (f *fakeEC2Client) TerminateInstances(_ context.Context, ids []string) error {
	if f.terminateErr != nil {
		return f.terminateErr
	}
	for _, id := range ids {
		if inst, ok := f.instances[id]; ok {
			inst.State = "terminated"
			f.instances[id] = inst
		}
	}
	return nil
}

func (f *fakeEC2Client) DescribeInstances(_ context.Context, tagFilters map[string]string) ([]ec2InstanceInfo, error) {
	if f.describeErr != nil {
		return nil, f.describeErr
	}
	var out []ec2InstanceInfo
	for _, inst := range f.instances {
		match := true
		for k, v := range tagFilters {
			if inst.Tags[k] != v {
				match = false
				break
			}
		}
		if match {
			out = append(out, inst)
		}
	}
	return out, nil
}

func minimalAWSCfg() config.AWSProviderConfig {
	return config.AWSProviderConfig{
		Region:        "us-east-1",
		InstanceType:  "t3.medium",
		SecurityGroup: "sg-123",
		SubnetID:      "subnet-456",
	}
}

// ---- Tests ----

func TestAWSProvider_Name(t *testing.T) {
	p := newAWSProviderWithClient(minimalAWSCfg(), newFakeEC2Client())
	if p.Name() != "aws" {
		t.Errorf("expected name %q, got %q", "aws", p.Name())
	}
}

func TestAWSProvider_SpinUp_ReturnsInstance(t *testing.T) {
	p := newAWSProviderWithClient(minimalAWSCfg(), newFakeEC2Client())

	req := SpinUpRequest{
		InstanceID: "aws-test-1",
		Pools:      []string{"default"},
		MasterAddr: "http://10.0.0.1:8080",
		Token:      "secret",
	}

	inst, err := p.SpinUp(context.Background(), req)
	if err != nil {
		t.Fatalf("SpinUp: %v", err)
	}

	if inst.ID != req.InstanceID {
		t.Errorf("expected ID %q, got %q", req.InstanceID, inst.ID)
	}
	if inst.ProviderID == "" {
		t.Error("ProviderID should not be empty")
	}
	if inst.InstanceType != "t3.medium" {
		t.Errorf("expected instance type t3.medium, got %q", inst.InstanceType)
	}
	if inst.Region != "us-east-1" {
		t.Errorf("expected region us-east-1, got %q", inst.Region)
	}
	if inst.Status != InstanceProvisioning {
		t.Errorf("expected status Provisioning, got %q", inst.Status)
	}
}

func TestAWSProvider_SpinUp_SelectsGPUInstanceType(t *testing.T) {
	cfg := minimalAWSCfg()
	cfg.GPUInstanceType = "g4dn.xlarge"
	p := newAWSProviderWithClient(cfg, newFakeEC2Client())

	req := SpinUpRequest{
		InstanceID: "gpu-inst",
		Pools:      []string{"gpu_workers"},
		MasterAddr: "http://host:8080",
		NeedsGPU:   true,
	}

	inst, err := p.SpinUp(context.Background(), req)
	if err != nil {
		t.Fatalf("SpinUp: %v", err)
	}
	if inst.InstanceType != "g4dn.xlarge" {
		t.Errorf("expected GPU instance type g4dn.xlarge, got %q", inst.InstanceType)
	}
}

func TestAWSProvider_SpinUp_FallsBackToDefaultWhenNoGPUType(t *testing.T) {
	cfg := minimalAWSCfg()
	// GPUInstanceType not set.
	p := newAWSProviderWithClient(cfg, newFakeEC2Client())

	req := SpinUpRequest{
		InstanceID: "gpu-fallback",
		Pools:      []string{"gpu_workers"},
		MasterAddr: "http://host:8080",
		NeedsGPU:   true,
	}

	inst, err := p.SpinUp(context.Background(), req)
	if err != nil {
		t.Fatalf("SpinUp: %v", err)
	}
	if inst.InstanceType != "t3.medium" {
		t.Errorf("expected fallback to t3.medium, got %q", inst.InstanceType)
	}
}

func TestAWSProvider_SpinUp_TagsInstance(t *testing.T) {
	fake := newFakeEC2Client()
	p := newAWSProviderWithClient(minimalAWSCfg(), fake)

	req := SpinUpRequest{
		InstanceID: "tagged-inst",
		Pools:      []string{"default"},
		MasterAddr: "http://host:8080",
	}
	_, err := p.SpinUp(context.Background(), req)
	if err != nil {
		t.Fatalf("SpinUp: %v", err)
	}

	for _, inst := range fake.instances {
		if inst.Tags["nagare:managed-by"] != "nagare-autoscaler" {
			t.Errorf("missing or wrong nagare:managed-by tag: %q", inst.Tags["nagare:managed-by"])
		}
		if inst.Tags["nagare:instance-id"] != "tagged-inst" {
			t.Errorf("missing or wrong nagare:instance-id tag: %q", inst.Tags["nagare:instance-id"])
		}
		if inst.Tags["nagare:pools"] != "default" {
			t.Errorf("missing or wrong nagare:pools tag: %q", inst.Tags["nagare:pools"])
		}
	}
}

func TestAWSProvider_SpinDown_TerminatesInstance(t *testing.T) {
	fake := newFakeEC2Client()
	p := newAWSProviderWithClient(minimalAWSCfg(), fake)

	inst, _ := p.SpinUp(context.Background(), SpinUpRequest{
		InstanceID: "to-terminate",
		Pools:      []string{"default"},
		MasterAddr: "http://host:8080",
	})

	if err := p.SpinDown(context.Background(), inst.ProviderID); err != nil {
		t.Fatalf("SpinDown: %v", err)
	}

	if fake.instances[inst.ProviderID].State != "terminated" {
		t.Errorf("expected state terminated, got %q", fake.instances[inst.ProviderID].State)
	}
}

func TestAWSProvider_List_ReturnsOnlyManagedInstances(t *testing.T) {
	fake := newFakeEC2Client()
	// Pre-add an unmanaged instance.
	fake.instances["unmanaged"] = ec2InstanceInfo{
		InstanceID: "unmanaged",
		Tags:       map[string]string{"other": "value"},
		State:      "running",
	}

	p := newAWSProviderWithClient(minimalAWSCfg(), fake)
	p.SpinUp(context.Background(), SpinUpRequest{ //nolint:errcheck
		InstanceID: "managed-1",
		Pools:      []string{"default"},
		MasterAddr: "http://host:8080",
	})

	instances, err := p.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(instances) != 1 {
		t.Errorf("expected 1 managed instance, got %d", len(instances))
	}
	if instances[0].ID != "managed-1" {
		t.Errorf("expected instance ID managed-1, got %q", instances[0].ID)
	}
}

func TestAWSProvider_List_ExcludesTerminated(t *testing.T) {
	fake := newFakeEC2Client()
	fake.instances["terminated-inst"] = ec2InstanceInfo{
		InstanceID: "terminated-inst",
		Tags: map[string]string{
			"nagare:managed-by":  "nagare-autoscaler",
			"nagare:instance-id": "some-id",
			"nagare:pools":       "default",
		},
		State: "terminated",
	}

	p := newAWSProviderWithClient(minimalAWSCfg(), fake)
	instances, err := p.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(instances) != 0 {
		t.Errorf("expected 0 instances (terminated excluded), got %d", len(instances))
	}
}

func TestAWSProvider_NewProvider_MissingRegion(t *testing.T) {
	cfg := config.AWSProviderConfig{
		InstanceType:  "t3.medium",
		SecurityGroup: "sg-123",
		SubnetID:      "subnet-456",
	}
	_, err := NewAWSProvider(cfg)
	if err == nil {
		t.Error("expected error for missing region")
	}
}

func TestAWSProvider_BuildUserData_ContainsCommand(t *testing.T) {
	p := newAWSProviderWithClient(minimalAWSCfg(), newFakeEC2Client())
	req := SpinUpRequest{
		MasterAddr: "http://10.0.0.1:8080",
		Pools:      []string{"default", "gpu_workers"},
		Token:      "tok123",
	}
	ud := p.buildUserData(req)

	checks := []string{
		"#!/bin/bash",
		"nagare",
		"--worker",
		"--join",
		"http://10.0.0.1:8080",
		"--pools",
		"default,gpu_workers",
		"--token",
		"tok123",
	}
	for _, want := range checks {
		if !strings.Contains(ud, want) {
			t.Errorf("user-data missing %q:\n%s", want, ud)
		}
	}
}

func TestAWSProvider_BuildUserData_IncludesDownloadWhenURLSet(t *testing.T) {
	cfg := minimalAWSCfg()
	cfg.NagareDownloadURL = "https://example.com/nagare"
	p := newAWSProviderWithClient(cfg, newFakeEC2Client())

	ud := p.buildUserData(SpinUpRequest{MasterAddr: "http://host:8080", Pools: []string{"default"}})
	if !strings.Contains(ud, "curl") {
		t.Errorf("expected curl in user-data when NagareDownloadURL is set:\n%s", ud)
	}
	if !strings.Contains(ud, "https://example.com/nagare") {
		t.Errorf("expected download URL in user-data:\n%s", ud)
	}
}

func TestAWSProvider_ProfileStoredInConfig(t *testing.T) {
	// Confirm that Profile round-trips through the config into the provider.
	// NewAWSProvider cannot be called without real AWS credentials, so we use
	// newAWSProviderWithClient and assert the cfg field is preserved.
	cfg := minimalAWSCfg()
	cfg.Profile = "staging"
	p := newAWSProviderWithClient(cfg, newFakeEC2Client())
	if p.cfg.Profile != "staging" {
		t.Errorf("expected cfg.Profile %q, got %q", "staging", p.cfg.Profile)
	}
}

func TestAWSProvider_NewProvider_MissingFieldsStillFailWithProfile(t *testing.T) {
	// A profile alone does not satisfy the required field validations.
	cfg := config.AWSProviderConfig{
		Profile:      "staging",
		InstanceType: "t3.nano",
		SubnetID:     "subnet-456",
		// SecurityGroup intentionally omitted
	}
	_, err := NewAWSProvider(cfg)
	if err == nil {
		t.Error("expected error for missing region even when profile is set")
	}
}
