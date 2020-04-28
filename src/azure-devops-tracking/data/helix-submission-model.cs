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

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

namespace models {

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

public class HelixWorkItemModel
{
    [JsonProperty(PropertyName = "id")]
    public string Id { get; set; }
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

public class HelixSubmissionModel
{
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
    public List<HelixWorkItemModel> WorkItems { get; set; }
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

} // end of namespace models

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
