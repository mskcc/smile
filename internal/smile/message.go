package smile

import (
	"github.com/google/uuid"
)

// generated from smile-server/service/src/test/resources/data/published_requests/outgoing_mocked_request2b_all_pairs.json using https://mholt.github.io/json-to-go/
type Request struct {
	SmileRequestID     uuid.UUID `json:"smileRequestId"`
	IgoRequestID       string    `json:"igoRequestId"`
	GenePanel          string    `json:"genePanel"`
	ProjectManagerName string    `json:"projectManagerName"`
	PiEmail            string    `json:"piEmail"`
	LabHeadName        string    `json:"labHeadName"`
	LabHeadEmail       string    `json:"labHeadEmail"`
	InvestigatorName   string    `json:"investigatorName"`
	InvestigatorEmail  string    `json:"investigatorEmail"`
	DataAnalystName    string    `json:"dataAnalystName"`
	DataAnalystEmail   string    `json:"dataAnalystEmail"`
	OtherContactEmails string    `json:"otherContactEmails"`
	DataAccessEmails   string    `json:"dataAccessEmails"`
	QcAccessEmails     string    `json:"qcAccessEmails"`
	IsCmoRequest       bool      `json:"isCmoRequest"`
	BicAnalysis        bool      `json:"bicAnalysis"`
	Samples            []Sample  `json:"samples"`
	PooledNormals      []string  `json:"pooledNormals"`
	IgoProjectID       string    `json:"igoProjectId"`
}
type QcReports struct {
	QcReportType         string `json:"qcReportType"`
	Comments             string `json:"comments"`
	InvestigatorDecision string `json:"investigatorDecision"`
}
type Runs struct {
	RunMode       string   `json:"runMode"`
	RunID         string   `json:"runId"`
	FlowCellID    string   `json:"flowCellId"`
	ReadLength    string   `json:"readLength"`
	RunDate       string   `json:"runDate"`
	FlowCellLanes []int    `json:"flowCellLanes"`
	Fastqs        []string `json:"fastqs"`
}
type Libraries struct {
	LibraryIgoID             string  `json:"libraryIgoId"`
	LibraryConcentrationNgul float64 `json:"libraryConcentrationNgul"`
	CaptureConcentrationNm   string  `json:"captureConcentrationNm"`
	CaptureInputNg           string  `json:"captureInputNg"`
	CaptureName              string  `json:"captureName"`
	Runs                     []Runs  `json:"runs"`
}
type CmoSampleIDFields struct {
	NaToExtract         string `json:"naToExtract"`
	SampleType          string `json:"sampleType"`
	NormalizedPatientID string `json:"normalizedPatientId"`
	Recipe              string `json:"recipe"`
}
type PatientAliases struct {
	Namespace string `json:"namespace"`
	Value     string `json:"value"`
}
type SampleAliases struct {
	Namespace string `json:"namespace"`
	Value     string `json:"value"`
}
type AdditionalProperties struct {
	IsCmoSample  string `json:"isCmoSample"`
	IgoRequestID string `json:"igoRequestId"`
}
type Sample struct {
	SmileSampleID        uuid.UUID            `json:"smileSampleId"`
	SmilePatientID       uuid.UUID            `json:"smilePatientId"`
	CmoSampleName        string               `json:"cmoSampleName"`
	SampleName           string               `json:"sampleName"`
	SampleType           string               `json:"sampleType"`
	OncotreeCode         string               `json:"oncotreeCode"`
	CollectionYear       string               `json:"collectionYear"`
	TubeID               string               `json:"tubeId"`
	CFDNA2DBarcode       string               `json:"cfDNA2dBarcode"` 
	QcReports            []QcReports          `json:"qcReports"`
	Libraries            []Libraries          `json:"libraries"`
	CmoPatientID         string               `json:"cmoPatientId"`
	PrimaryID            string               `json:"primaryId"`
	InvestigatorSampleID string               `json:"investigatorSampleId"`
	Species              string               `json:"species"`
	Sex                  string               `json:"sex"`
	TumorOrNormal        string               `json:"tumorOrNormal"`
	Preservation         string               `json:"preservation"`
	SampleClass          string               `json:"sampleClass"`
	SampleOrigin         string               `json:"sampleOrigin"`
	TissueLocation       string               `json:"tissueLocation"`
	BaitSet              string               `json:"baitSet"`
	GenePanel            string               `json:"genePanel"`
	Datasource           string               `json:"datasource"`
	IgoComplete          bool                 `json:"igoComplete"`
	CmoSampleIDFields    CmoSampleIDFields    `json:"cmoSampleIdFields"`
	PatientAliases       []PatientAliases     `json:"patientAliases"`
	SampleAliases        []SampleAliases      `json:"sampleAliases"`
	AdditionalProperties AdditionalProperties `json:"additionalProperties"`
}
