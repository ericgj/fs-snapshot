# Monitor

A domain-specific tool to monitor ERT CDM study files, built on a generic 
file-monitoring tool.

--------------------------------------------------------------------------------

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


