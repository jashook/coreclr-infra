////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
//
// Module: helix-submission-model
//
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

using System;
using System.Collections.Generic;

using Newtonsoft.Json;

using ev27;

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

namespace models {

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

public class HelixWorkItemModel : IDocument
{
    [JsonProperty(PropertyName = "id")]
    public string Id { get; set; }
    public string HelixSubmissionId { get; set; }
    public string StepId { get; set; }
    public string JobId { get; set; }
    public string RuntimePipelineId { get; set; }
    public string Console { get; set; }
    public double ElapsedSetupTime { get; set; }
    public double ElapsedRunTime { get; set; }
    public int ExitCode { get; set; }
    public string JobName { get; set; }
    public DateTime HelixWorkItemSetupBegin { get; set; }
    public DateTime HelixWorkItemSetupEnd { get; set; }

    public string MachineName { get; set; }

    [JsonProperty(PropertyName = "Name")]
    public string Name { get; set; }
    public DateTime RunBegin { get; set; }
    public DateTime RunEnd { get; set; }

    public override string ToString()
    {
        return JsonConvert.SerializeObject(this);
    }
    
}

public class HelixSubmissionModel : IDocument
{
    [JsonProperty(PropertyName = "id")]
    public string Id { get; set; }
    public string Console { get; set; }
    public double ElapsedTime { get; set; }
    public DateTime End { get; set; }
    public string JobName { get; set; }
    public string StepId { get; set; }
    public string Name { get; set; }
    public bool Passed { get; set; }
    public List<string> Queues { get; set; }
    public string Source { get; set; }
    public DateTime Start { get; set; }
    public string Type { get; set; }
    public int WorkItemCount { get; set; }
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

} // end of namespace models

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
