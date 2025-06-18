package common

import (
	"os"

	pb "github.com/learningfun-dev/NimbusGRPC/nimbus/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Hostname stores the name of the host/pod running the service.
// It is initialized once when the package is first loaded.
var Hostname string

// init runs automatically when the package is imported, setting up package-level variables.
func init() {
	var err error
	Hostname, err = os.Hostname()
	if err != nil {
		// Fallback if getting the hostname fails
		Hostname = "unknown-host"
	}
}

// TraceStepInfo holds the structured information for a single trace step.
type TraceStepInfo struct {
	ServiceName string
	MethodName  string
	Message     string
	Metadata    map[string]string
}

// Append takes a LogEntry and appends a new, structured trace step to it.
// It gracefully handles a nil input by creating a new LogEntry.
func Append(logEntry *pb.LogEntry, info TraceStepInfo) *pb.LogEntry {
	// If the incoming LogEntry is nil, initialize it.
	if logEntry == nil {
		logEntry = &pb.LogEntry{}
	}

	// Create a new structured trace step.
	step := &pb.TraceStep{
		Timestamp:   timestamppb.Now(), // Use the standard protobuf timestamp.
		ServiceName: info.ServiceName,
		InstanceId:  Hostname,
		MethodName:  info.MethodName,
		Message:     info.Message,
		Metadata:    info.Metadata,
	}

	// Append the new step to the list.
	logEntry.Steps = append(logEntry.Steps, step)

	return logEntry
}
