package contracts_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

const schemaBaseURL = "https://nagare.dev/contracts/v1/"

var contractNames = []string{
	"approval",
	"artifact",
	"assignment",
	"capability-request",
	"evaluation",
	"pack",
	"receipt",
}

var schemaResources = append(append([]string{}, contractNames...), "common")

var scenarios = []string{"software-change", "ml-experiment"}

func TestV1ContractsAcceptReferenceScenarios(t *testing.T) {
	compiled := compileContracts(t)

	for _, scenario := range scenarios {
		for _, contract := range contractNames {
			t.Run(scenario+"/"+contract, func(t *testing.T) {
				document := readDocument(t, filepath.Join("fixtures", scenario, contract+".json"))
				if err := compiled[contract].Validate(document); err != nil {
					t.Fatalf("validate reference fixture: %v", err)
				}
			})
		}
	}
}

func TestV1ContractsRejectMissingSafetyFields(t *testing.T) {
	compiled := compileContracts(t)
	tests := []struct {
		name     string
		contract string
		field    string
	}{
		{name: "assignment budget", contract: "assignment", field: "budget"},
		{name: "assignment output contract", contract: "assignment", field: "output_contract"},
		{name: "assignment capability declaration", contract: "assignment", field: "capabilities"},
		{name: "pack capability declaration", contract: "pack", field: "capabilities"},
		{name: "approval proposal digest", contract: "approval", field: "proposal_digest"},
		{name: "approval idempotency key", contract: "approval", field: "idempotency_key"},
		{name: "receipt idempotency key", contract: "receipt", field: "idempotency_key"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := readDocument(
				t,
				filepath.Join("fixtures", "software-change", test.contract+".json"),
			)
			delete(document, test.field)

			err := compiled[test.contract].Validate(document)
			if err == nil {
				t.Fatalf("accepted document without required field %q", test.field)
			}
			if !strings.Contains(err.Error(), test.field) {
				t.Fatalf("validation error %q does not identify missing field %q", err, test.field)
			}
		})
	}
}

func compileContracts(t *testing.T) map[string]*jsonschema.Schema {
	t.Helper()

	compiler := jsonschema.NewCompiler()
	for _, name := range schemaResources {
		path := filepath.Join("schemas", name+".schema.json")
		document, err := os.Open(path)
		if err != nil {
			t.Fatalf("open schema %s: %v", name, err)
		}
		if err := compiler.AddResource(schemaBaseURL+name+".schema.json", document); err != nil {
			_ = document.Close()
			t.Fatalf("register schema %s: %v", name, err)
		}
		if err := document.Close(); err != nil {
			t.Fatalf("close schema %s: %v", name, err)
		}
	}

	compiled := make(map[string]*jsonschema.Schema, len(contractNames))
	for _, name := range contractNames {
		schema, err := compiler.Compile(schemaBaseURL + name + ".schema.json")
		if err != nil {
			t.Fatalf("compile schema %s: %v", name, err)
		}
		compiled[name] = schema
	}
	return compiled
}

func readDocument(t *testing.T, path string) map[string]any {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	if document == nil {
		t.Fatalf("decode %s: expected JSON object", path)
	}
	return document
}
