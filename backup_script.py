#!/usr/bin/env python

"""
ideas for improvements
    - extras stuffs to save:
        - Files

        Other dump classes:
        - APIKeys
        - UserOpenID
        - Pages
        - Jobs / Tasks
        - Quotas
        - Group
        - GalaxySession
        - Role

    - rules to save datas:
        - selective backup : on all users, or on a user, on an history
        - last access / modication timestamp: give the possibility to the
          admin to choose by access time
        - If backup with files: select by size of dataset, or by size of
          history: give the possibility to the admin to choose a max size

"""


#export retrieved data to json file
from json import dumps

import decimal
#import datetime
import argparse
import sys
import os

NUM_USERS = None


def check_python_version():
    if sys.version_info < (2, 7, 0):
        sys.exit("You need python 2.7 (because of argparse) or later to run this script\n")


def decimal_default(obj):
    """
    Convert Decimal type object to a std float
    """
    #if isinstance(obj, datetime.datetime):
        # need to cast datetime object in string...
    #    pass
    #elif isinstance(obj, decimal.Decimal):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError(type(obj))


def check_galaxy_root_dir():
    """
    Need this function before importing galaxy model and db_shell
    Otherwise, it will raise an error.
    """
    #~ scripts_dir = os.path.abspath( os.path.dirname( sys.argv[0] ) )
    existing_tests = []
    existing_tests.append(os.path.isfile("run.sh"))
    existing_tests.append(os.path.isfile("run_reports.sh"))
    existing_tests.append(os.path.isfile("run_tool_shed.sh"))
    existing_tests.append(os.path.isfile("run_tests.sh"))
    existing_tests.append(os.path.isfile("manage_db.sh"))
    existing_tests.append(os.path.isfile("create_db.sh"))
    if False in existing_tests:
        sys.exit("You need to launch this script in GALAXY_ROOT_DIR (or some galaxy files missing ?)\n")
    #~ else:
        #~ print ("Ok, Seems to be in the good directory!")



# need to launch that check before importing any galaxy python stuff
check_galaxy_root_dir()


# begin of the interesting part here
from scripts.db_shell import *





def retrieve_datasets(nd,np):
    """
    Retrieve datasets
    """
    datasets = []
    datasetsRoot = {'datasets':datasets}

    NUM_DAT = sa_session.query(Dataset).count()
    CLEAN_NUM_DAT = 0

    ## this part does not work if there is a hole in ID count.
    ## e.g. : deleting users directly in DB
    #~ for i in range(NUM_DAT):
        #~ dat = sa_session.query(Dataset).get(i+1)
        #~ if dat is not None:
            # do some stuff
            # pass
        #~ else:
            #~ break

    all_datasets = sa_session.query(Dataset).order_by(Dataset.id)
    for dat in all_datasets:
        id = dat.id
        state = dat.state
        deleted = dat.deleted
        purged = dat.purged
        purgable = dat.purgable
        external_filename = dat.external_filename
        external_extra_files_path = ""
        if hasattr(dat, 'external_extra_files_path'):
            external_extra_files_path = dat.external_extra_files_path
        _extra_files_path = ""
        if hasattr(dat, '_extra_files_path'):
            _extra_files_path = dat._extra_files_path
        file_size = dat.file_size
        if purged == True and np == True:
            continue
        elif deleted == True and nd == True:
            continue
        else:
            datasets.append( { 'id':id, 'state':state, 'deleted': deleted, \
            'purged':purged, 'purgable':purgable, 'external_filename':external_filename, \
            'external_filename':external_filename, 'external_extra_files_path':\
            external_extra_files_path, 'extra_files_path':_extra_files_path, \
            'file_size':file_size } )
            CLEAN_NUM_DAT = CLEAN_NUM_DAT + 1

    return datasetsRoot, NUM_DAT, CLEAN_NUM_DAT



def retrieve_histories(nd,np):
    """
    Retrieve histories
    """
    histories = []
    historiesRoot = {'histories':histories}
    NUM_HIST = sa_session.query(History).count()
    CLEAN_NUM_HIST = 0

    all_histories = sa_session.query(History).order_by(History.id)
    for hid in all_histories:
        id = hid.id
        name = hid.name
        tags = hid.tags
        deleted = hid.deleted
        purged = hid.purged
        importing = hid.importing
        genome_build = hid.genome_build
        published = hid.published
        # Relationships
        user = hid.user
        if user is not None:
            email = user.email
        else:
            email = ""
        datasets = hid.datasets
        datasetnames = []
        if len(datasets) > 0:
            for dataset in datasets:
                datasetnames.append(dataset.name)
        # Here you can optionnaly retrieve session informations from galaxy.model.GalaxySessionToHistoryAssociation object (session_key, is_valid, remote_addr...)
        #~ galaxy_sessions = str(hid.galaxy_sessions)
        if purged == True and np == True:
            continue
        elif deleted == True and nd == True:
            continue
        else:
            histories.append( { 'id':id, 'name':name, 'email':email, \
            'datasetnames':datasetnames, 'tags':tags, 'deleted':deleted, \
            'purged':purged, 'importing':importing, 'genome_build':genome_build, \
            'published':published })
            CLEAN_NUM_HIST = CLEAN_NUM_HIST + 1

    return historiesRoot, NUM_HIST, CLEAN_NUM_HIST



def retrieve_libraries(nd,np):
    """
    Retrieve libraries
    """
    libraries = []
    librariesRoot = {'libraries':libraries}
    NUM_LIB = sa_session.query(Library).count()

    all_libraries = sa_session.query(Library).order_by(Library.name)
    for lib in all_libraries:
        libdict = lib.to_dict()
        if hasattr(lib, 'description'):
            libdict['description'] = lib.description
        if hasattr(lib, 'synopsis'):
            libdict['synopsis'] = lib.synopsis
        if hasattr(lib, 'root_folder'):
            libdict['root_folder_id'] = lib.root_folder.id
            libdict['root_folder_name'] = lib.root_folder.name
            libdict['root_folder_description'] = lib.root_folder.description
            libdict['root_folder_item_count'] = lib.root_folder.item_count
            libdict['root_folder_order_id'] = lib.root_folder.order_id
            libdict['root_folder_genome_build'] = lib.root_folder.genome_build
        libraries.append(libdict)

    return librariesRoot, NUM_LIB



def retrieve_libraryDatasetDatasetAssociations(nd,np):
    """
    Retrieve LibraryDataset objects
    """
    libraryDatasetDatasetAssociations = []
    libraryDatasetDatasetAssociationRoot = {'libraryDatasetDatasetAssociations':libraryDatasetDatasetAssociations}

    NUM_LDDA = sa_session.query(LibraryDatasetDatasetAssociation).count()

    ## LibraryDatasetDatasetAssociation
    all_LibraryDatasetDatasetAssociation = sa_session.query(LibraryDatasetDatasetAssociation).filter_by(deleted='False')
    for ld in all_LibraryDatasetDatasetAssociation:
        lddict = ld.to_dict()
        libraryDatasetDatasetAssociations.append(lddict)

    return libraryDatasetDatasetAssociationRoot, NUM_LDDA



def retrieve_libraryDatasets(nd,np):
    """
    Retrieve LibraryDataset objects
    """
    libraryDatasets = []
    libraryDatasetsRoot = {'libraryDatasets':libraryDatasets}

    NUM_LD = sa_session.query(LibraryDataset).count()

    ## LibraryDataset
    all_LibraryDatasets = sa_session.query(LibraryDataset).filter_by(deleted='False')
    for ld in all_LibraryDatasets:
        lddict = ld.to_dict()
        libraryDatasets.append(lddict)

    return libraryDatasetsRoot, NUM_LD



def retrieve_libraryFolders(nd,np):
    """
    Retrieve LibraryFolders objects
    """
    libraryFolders = []
    libraryFoldersRoot = {'libraryFolders':libraryFolders}

    NUM_LF = sa_session.query(LibraryFolder).count()

    ## LibraryFolder
    all_libraryFolders = sa_session.query(LibraryFolder).all()
    for lf in all_libraryFolders:
        lfdict = {'name':lf.name, 'id':lf.id, 'item_count':lf.item_count, 'order_id':lf.order_id, \
        'description':lf.description, 'genome_build':lf.genome_build, 'update_time':str(lf.update_time), \
        'parent_id':lf.parent_id}
        libraryFolders.append(lfdict)

    return libraryFoldersRoot, NUM_LF


def retrieve_users(nd,np):
    """
    Retrieve users from previous galaxy
    """
    global NUM_USERS
    CLEAN_NUM_USERS = 0

    users = []
    usersRoot = {'users':users}
    if NUM_USERS is None:
        NUM_USERS = sa_session.query(User).count()

    all_users = sa_session.query(User).order_by(User.id)
    for uid in all_users:
        histories_names = []
        histories_ids = []
        id = uid.id
        email = uid.email
        hashpassword = uid.password
        external = uid.external
        deleted = uid.deleted
        purged = uid.purged
        active = uid.active
        activation_token = uid.activation_token
        username = uid.username
        # Relationships
        for uidh in uid.histories:
            histories_names.append(uidh.name)
            histories_ids.append(uidh.id)
        if purged == True and np == True:
            continue
        elif deleted == True and nd == True:
            continue
        else:
            users.append( { 'id':id, 'email':email, 'username':username, \
            'hashpassword':hashpassword, 'external':external, 'deleted':deleted, \
            'purged':purged, 'active':active, 'activation_token':activation_token, \
            'histories_names':histories_names, 'histories_ids':histories_ids } )
            CLEAN_NUM_USERS = CLEAN_NUM_USERS + 1

    return usersRoot, NUM_USERS, CLEAN_NUM_USERS



def retrieve_workflows(nd,np):
    """
    Retrieve workflows with all steps
    """
    workflows = []
    workflowsRoot = {'workflows':workflows}

    NUM_WF = sa_session.query(Workflow).count()

    ## storedWorkflows
    all_stored_workflows = sa_session.query(StoredWorkflow).order_by(StoredWorkflow.id)
    for swf in all_stored_workflows:
        swfdict = swf.to_dict()
        if hasattr(swf, 'latest_workflow_id'):
            swfdict['latest_workflow_id'] = swf.latest_workflow_id
        if hasattr(swf, 'slug'):
            swfdict['slug'] = swf.slug
        if hasattr(swf, 'user'):
            if swf.user is not None:
                swfdict['user_email'] = swf.user.email
        #~ if hasattr(swf, 'workflows'):
            #~ print(repr(swf.workflows))
        workflows.append(swfdict)

    ## workflow objects
    all_workflows = sa_session.query(Workflow).order_by(Workflow.name)
    for wf in all_workflows:
        wfdict = wf.to_dict()
        if hasattr(wf, 'user'):
            if wf.user is not None:
                wfdict['user_email'] = wf.user.email
        if hasattr(wf, 'uuid'):
            wfdict['uuid'] = wf.uuid
        wfdict['wst_id'] = []
        if hasattr(wf, 'steps'):
            for wst in wf.steps:
                wfdict['wst_id'].append(wst.id)
        workflows.append(wfdict)

    ## steps
    all_workflows_steps = sa_session.query(WorkflowStep).order_by(WorkflowStep.id)
    wfs = []
    for wf_step in all_workflows_steps:
        workflow_id = wf_step.workflow.id
        wfs_id = wf_step.id
        wfs_type = wf_step.type
        wfs_tool_id = wf_step.tool_id
        wfs_tool_inputs = wf_step.tool_inputs
        wfs_tool_errors = wf_step.tool_errors
        wfs_position = wf_step.position
        wfs_input_connections = []
        if len(wf_step.input_connections) > 0:
            for wfs_ic in wf_step.input_connections:
                wfs_input_connections.append({ \
                'input_name':wfs_ic.input_name, \
                'input_step_id':wfs_ic.input_step_id, \
                'output_name':wfs_ic.output_name, \
                'output_step_id':wfs_ic.output_step_id
                })
        wfs_config = wf_step.config
        wfs.append({ 'id':wfs_id, 'type':wfs_type, 'workflow_id':workflow_id, \
        'model_class':'WorkflowStep', 'tool_id':wfs_tool_id, \
        'tool_inputs':wfs_tool_inputs, 'tool_errors':wfs_tool_errors, \
        'position':wfs_position, 'input_connections':wfs_input_connections, \
        'config':wfs_config })
    workflows.append(wfs)

    ## output objects
    all_WorkflowOutput = sa_session.query(WorkflowOutput).order_by(WorkflowOutput.output_name)
    for wfo in all_WorkflowOutput:
        workflows.append( {'output_name':wfo.output_name, \
        'model_class':'WorkflowOutput', 'workflow_step_id':wfo.workflow_step.id })

    ## invocations
    #~ all_WorkflowInvocation = sa_session.query(WorkflowInvocation).order_by(WorkflowInvocation.id)
    #~ for wfi in all_WorkflowInvocation:
        #~ workflows.append( wfi.to_dict() )
    ## invocations step
    #~ all_WorkflowInvocationStep = sa_session.query(WorkflowInvocationStep).order_by(WorkflowInvocationStep.id)
    #~ for wfis in all_WorkflowInvocationStep:
        #~ workflows.append( wfis.to_dict() )

    return workflowsRoot, NUM_WF



if __name__ == '__main__':
    """
    Main code to call the backup you need.
    < Takes an argument to specify the data to backup
    > Returns the data in json
    """

    check_python_version()


    nd = False
    np = False
    verbose = False
    outfile = False

    usage = "usage :\n\
    \n\
    cd /path/to/galaxy \n\n\
    && \n\
    python backup_script.py -b <all|users|workflows|libraries|histories|datasets>\n\
    (-o --outfile /path/to/filename)\n\
    (-np --nopurged)\n\
    (-nd --nodeleted)\n\
    (-v --verbose)\n\n\
    -help\n"
    epilog = "galaxy backup_script. A beta"\
    + " release from Remy Dernat, Evolution Science Institute -"\
    + " Montpellier, France."

    parser = argparse.ArgumentParser(description=usage, epilog=epilog)
    parser.add_argument('--version', action='version', version='%(prog)s 0.1')

    parser.add_argument('-o', '--outfile', \
    type=argparse.FileType('wb', 0), help = "JSON filename to create")
    parser.add_argument('-np', '--nopurged', action='store_true', \
    help = "do not backup purged elements")
    parser.add_argument('-nd', '--nodeleted', action='store_true', \
    help = "do not backup deleted elements")
    parser.add_argument('-v', '--verbose', action='store_true', \
    help = "Display many informations except json datas if you choose output file [-o]")
    parser.add_argument('-b', '--backup', choices=['users', 'workflows', \
    'libraries', 'histories', 'datasets', 'all'], help = "The data to backup", \
    required=True)

    args = parser.parse_args()

    backup2extract = args.backup

    try:
        if(args.outfile):
            outfile = args.outfile
    except:
        outfile = False

    if(args.nopurged):
        np = args.nopurged

    if(args.nodeleted):
        nd = args.nodeleted

    if(args.verbose):
        verbose = args.verbose

    backup = []

    if backup2extract == "users" or backup2extract == "all":
        users, num_users, clean_num_users = retrieve_users(nd,np)
        if verbose:
            print("\n####################################\n")
            print("%s USERS RETRIEVED" %(num_users))
            print( "%s CLEAN USERS PROCESSED" %(clean_num_users) )
        backup.append(users)

    if backup2extract == "histories" or backup2extract == "all":
        histories, num_hist, clean_num_hist = retrieve_histories(nd,np)
        if verbose:
            print("\n####################################\n")
            print( "%s HISTORIES RETRIEVED" %(num_hist) )
            print( "%s CLEAN HISTORIES PROCESSED" %(clean_num_hist) )
        backup.append(histories)

    if backup2extract == "datasets" or backup2extract == "all":
        datasets, num_dat, clean_num_dat = retrieve_datasets(nd,np)
        if verbose:
            print("\n####################################\n")
            print( "%s DATASETS RETRIEVED" %(num_dat) )
            print( "%s CLEAN DATASETS PROCESSED" %(clean_num_dat) )
        backup.append(datasets)

    if backup2extract == "workflows" or backup2extract == "all":
        workflows, num_wf = retrieve_workflows(nd,np)
        if verbose:
            print("\n####################################\n")
            print( "%s WORKFLOWS RETRIEVED" %(num_wf) )
        backup.append(workflows)

    if backup2extract == "libraries" or backup2extract == "all":
        libraries, num_lib = retrieve_libraries(nd,np)
        libraryDatasets, num_ld = retrieve_libraryDatasets(nd,np)
        libraryDatasetDatasetAssociations, num_ldda = \
        retrieve_libraryDatasetDatasetAssociations(nd,np)
        libraryFolders, num_lf = retrieve_libraryFolders(nd,np)
        if verbose:
            print("\n####################################\n")
            print( "%s LIBRARIES RETRIEVED" %(num_lib) )
            print( "%s LIB_DATASETS RETRIEVED" %(num_ld) )
            print( "%s LIB_DATASET_ASSOCIATIONS RETRIEVED" %(num_ldda) )
            print( "%s LIB_FOLDERS RETRIEVED" %(num_lf) )
        backup.append(libraryFolders)
        backup.append(libraries)
        backup.append(libraryDatasets)
        backup.append(libraryDatasetDatasetAssociations)


    backup = dumps(backup, default=decimal_default, sort_keys=True, indent=4)

    if not outfile:
        print(backup)
    else:
        outfile.write(backup)
        outfile.close()
