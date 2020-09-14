# Monitor

A domain-specific tool to monitor ERT CDM study files, built on a generic 
file-monitoring tool.

--------------------------------------------------------------------------------

_13 Sept 2020_

# The more urgent use

...is monitoring disk usage, both a current snapshot and over time. I'm 
imagining a basic report that looks like this for 'current snapshot':

    Data Type   System      File Type   Size     Largest Study
    ABPM        ABPM                    201 GB   089123
                            Extract     130 GB   089123
                            Final        61 GB   089456
                            Interim      10 GB   089678

or maybe 'Largest Study' is not as helpful here; we could have a separate 
listing not by File Type but by "top ten" largest and oldest studies per
Data Type + System:

    Data Type   System      Study       Size     Last Modified
    ABPM        ABPM        089123       16 GB   06 MAY 2017

"Largest and oldest" meaning not, first the largest, and then the oldest within
the largest; but treating file age and size as equal factors. We are just as
interested in finding the oldest studies to archive as the largest. This kind
of query could be done using `RANK()` in a subquery and then `MAX(age_rank +
file_age_rank)`, I think.

But actually it might be simpler than this. I think the rule for looking at
studies to archive is over 2 years past database lock. So we could simply
filter the list down by age and sort by size.

One problem I see is the storage db is generic over the file metadata. 
You can do Data Type, System, File Type by defining separate searches for
each (which end up as predictable tags per import). But as for grouping by
pieces of the file metadata, which you don't know ahead of time, how are you 
going to do that?

I think what could be done is to introduce the 'project' as generic concept.
Each file record in the database stores its 'project' in a full-fledged field
(i.e. not in tags), which can then be indexed and queried. The search specs
can then (similar to the `archived` business) define which metadata fields
make up the 'project'.

    [fs-snapshot:ecg/extract]
    metadata = 
        data_type = ECG
        file_type = Extract
    root_dir = E:\\community\\ecg
    match_paths =
        {account}\\csv\\{protocol}_{qc_or_pr}_C_*.CSV
        {account}\\csv\\{archive}\\{protocol}_{qc_or_pr}_C_*.CSV
    archived =
        has-metadata archive "archived,archive,_archive,_archived"
    project = from-metadata "account,protocol"

Thinking about it, we could also introduce a generic concept of 'file type'
at the file level, stored in a separate field. So for example:

    project = from-metadata "account,protocol"
    file_type = from-metadata "qc_or_pr"

Then we can query file types across projects and projects across file types
without difficulty.



_11 Sept 2020_

# Splitting out the generic tool further

I think the generic tool is useful enough in its own right to be split out
into its own library and/or command-line app. Given it does some heavy I/O
between the file system crawling, database access, threads, etc., my tendency
is to see it as an app rather than a library. But the thing is, the crawler
(`save`) is an app, as is the diff. But on the 'subscriber' side, it basically
just needs access to the FileInfo model and an update function that processes
Actions against FileInfos: it needs a library.

## Configuration

If we're talking about an app, we need a serialization of the Config and
SearchSpec. The list of match paths and dict of metadata make it a bit 
awkward to specify on the command line.

    fs-snapshot save
        --root-dir "E:\\community\\ecg"
        --metadata "data_type:ECG"
        --metadata "file_type:Extract"
        --match_path "{account}\\csv\\{protocol}_{qc_or_pr}_C_*.CSV"
        --match_path "{account}\\csv\\{archive}\\{protocol}_{qc_or_pr}_C_*.CSV"

Thus perhaps a single ini config file:

    [fs-snapshot]
    db_root_dir =  "." 
    db_base_name = "fs-snapshot.sqlite"

    [fs-snapshot:ecg/extract]
    metadata = 
        data_type = ECG
        file_type = Extract
    root_dir = E:\\community\\ecg
    match_paths =
        {account}\\csv\\{protocol}_{qc_or_pr}_C_*.CSV
        {account}\\csv\\{archive}\\{protocol}_{qc_or_pr}_C_*.CSV


And then:

    fs-snapshot save "ecg/extract" --config my-config.ini

    fs-snapshot diff e6a435021bc8f --config my-config.ini


As for the lambda involved in "what makes it an archive". Let's say the
commonest cases are "is this variable included in the metadata" and "if the
value is among possible choices":

    [fs-snapshot:ecg/extract]
    metadata = 
        data_type = ECG
        file_type = Extract
    root_dir = E:\\community\\ecg
    match_paths =
        {account}\\csv\\{protocol}_{qc_or_pr}_C_*.CSV
        {account}\\csv\\{archive}\\{protocol}_{qc_or_pr}_C_*.CSV
    archived =
        has-metadata archive "archived,archive,_archive,_archived"


## Subscriber side

I think the main useability thing is to make the necessary models and functions
available from the top level of the library:

    from fs_snapshot import Action, update


