////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
//
// Module: runtime-model.cs
//
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

using System;
using System.Collections.Generic;

using DevOps.Util;

using Newtonsoft.Json;

using ev27;

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

namespace models {

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

public class RuntimeModel : IDocument
{
    [JsonProperty(PropertyName = "id")]
    public string Id { get; set; }

    public string Console { get; set; }

    public DateTime DateStart { get; set; }

    public DateTime DateEnd { get; set; }

    public long ElapsedTime { get; set; }

    public BuildResult BuildResult { get; set; }

    public List<AzureDevOpsJobModel> Jobs { get; set; }

    public string BuildNumber { get; set; }
    public string BuildUri { get; set; }

    public BuildReason BuildReason { get; set; }

    [JsonProperty(PropertyName = "BuildReasonString")]
    public string BuildReasonString { get; set; }

    public string Name { get; set; }

    public string PrNumber { get; set; }

    public string PrSourceBranch { get; set; }

    public string PrSourceSha { get; set; }

    public string SourceSha { get; set; }
    public string SourceBranch { get; set; }
    public string PrTitle { get; set; }

    public string PrSenderName { get; set; }

    public bool IsCoreclrRun { get; set; }

    public bool IsLibrariesRun { get; set; }

    public bool IsInstallerRun { get; set; }

    public bool IsMonoRun { get; set; }

    public override string ToString()
    {
        return JsonConvert.SerializeObject(this);
    }

}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

} // end of namespace azure-devops-tracking.models

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
