#!/usr/bin/env python

"""
ideas for improvements
    - extras stuffs to save:
        - Files

        Other dump classes:
        - UserOpenID
        - Pages
        - Jobs / Tasks
        - Quotas
        - GalaxySession

    - rules to save datas:
        - selective backup : on all users, or on a user, on an history
        - last access / modication timestamp: give the possibility to the
          admin to choose by access time
        - If backup with files: select by size of dataset, or by size of
          history: give the possibility to the admin to choose a max size

    - QT interface ?

"""


#export retrieved data to json file
from json import dumps

import decimal
#import datetime
import argparse
import sys
import os
import datetime


NUM_USERS = None


def check_python_version():
    if sys.version_info < (2, 7, 0):
        sys.exit("You need python 2.7 (because of argparse) or later to"+\
        " run this script\n")


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
        sys.exit("You need to launch this script in GALAXY_ROOT_DIR (or"+\
        " some galaxy files missing ?)\n")
    #~ else:
        #~ print ("Ok, Seems to be in the good directory!")



# need to launch that check before importing any galaxy python stuff
check_galaxy_root_dir()


# begin of the interesting part here
from scripts.db_shell import *




def retrieve_apikeys(nd, np):
    """
    Retrieve API Keys()
    """
    apikeys = []
    apikeysRoot = {'api_keys':apikeys}

    NUM_KEYS = sa_session.query(APIKeys).count()
    all_keys = sa_session.query(APIKeys).all()

    for the_key in all_keys:
        id = the_key.id
        user_id = the_key.user_id
        key = the_key.key
        apikeys.append({'id':id, 'user_id':user_id, 'key':key})

    return apikeysRoot, NUM_KEYS



def retrieve_groups(nd, np):
    """
    Retrieve Groups()
    """
    groups = []
    groupsRoot = {'groups':groups}

    NUM_GROUPS = sa_session.query(Group).count()
    all_groups = sa_session.query(Group).all()

    for the_group in all_groups:
        id = the_group.id
        name = the_group.name
        deleted = the_group.deleted
        if deleted is False:
            groups.append({'id':id, 'name':name, 'deleted':deleted})
        elif deleted is True and nd is False:
            groups.append({'id':id, 'name':name, 'deleted':deleted})
        else:
            continue


    return groupsRoot, NUM_GROUPS



def retrieve_roles(nd, np):
    """
    Retrieve Roles()
    """
    roles = []
    rolesRoot = {'roles':roles}

    NUM_ROLES = sa_session.query(Role).count()
    all_roles = sa_session.query(Role).all()

    for the_role in all_roles:
        id = the_role.id
        name = the_role.name
        description = the_role.description
        role_type = the_role.type
        deleted = the_role.deleted
        if deleted is False:
            roles.append({'id':id, 'name':name, 'description':description, \
            'type':role_type, 'deleted':deleted})
        elif deleted is True and nd is False:
            roles.append({'id':id, 'name':name, 'description':description, \
            'type':role_type, 'deleted':deleted})
        else:
            continue

    return rolesRoot, NUM_ROLES



def retrieve_associations():
    """
    Associations
        #~ UserGroupAssociation
        #~ UserRoleAssociation
        #~ GroupRoleAssociation
    """
    all_UGAs = sa_session.query(UserGroupAssociation).all()
    all_URAs = sa_session.query(UserRoleAssociation).all()
    all_GRAs = sa_session.query(GroupRoleAssociation).all()
    UGAs = []
    URAs = []
    GRAs = []
    assoRoot = {'UserGroupAssociation':UGAs, 'UserRoleAssociation':URAs, \
    'GroupRoleAssociation':GRAs}

    for uga in all_UGAs:
        user__email = uga.user.email
        group__name = uga.group.name
        UGAs.append({'user__email':user__email, 'group__name':group__name})

    for ura in all_URAs:
        user__email = ura.user.email
        role__name = ura.role.name
        URAs.append({'user__email':user__email, 'role__name':role__name})

    for gra in all_GRAs:
        group__name = gra.group.name
        role__name = gra.role.name
        GRAs.append({'group__name':group__name, 'role__name':role__name})

    return assoRoot



def retrieve_datasets(nd, np):
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
            datasets.append({'id':id, 'state':state, 'deleted': deleted, \
            'purged':purged, 'purgable':purgable, 'external_filename':\
            external_filename, 'external_filename':external_filename, \
            'external_extra_files_path':external_extra_files_path, \
            'extra_files_path':_extra_files_path, 'file_size':file_size})
            CLEAN_NUM_DAT = CLEAN_NUM_DAT + 1

    return datasetsRoot, NUM_DAT, CLEAN_NUM_DAT



def retrieve_datasetPermissions(nd, np):
    """
    Retrieve DatasetPermissions
    """
    datasetPermissions = []
    datasetPermissionsRoot = {'datasetPermissions':datasetPermissions}
    all_datasetPermissions = sa_session.query(DatasetPermissions).all()
    for dp in all_datasetPermissions:
        action = dp.action
        dataset__external_filename = dp.dataset.external_filename
        role__name = dp.role.name
        datasetPermissions.append({'action':action, \
        'dataset__external_filename':dataset__external_filename, \
        'role__name':role__name})

    return datasetPermissionsRoot



def retrieve_histories(nd, np):
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
            user__email = user.email
        else:
            user__email = ""
        datasets = hid.datasets
        datasetnames = []
        if len(datasets) > 0:
            for dataset in datasets:
                datasetnames.append(dataset.name)
        # Here you can optionnaly retrieve session informations from
        # galaxy.model.GalaxySessionToHistoryAssociation object
        # (session_key, is_valid, remote_addr...)
        #~ galaxy_sessions = str(hid.galaxy_sessions)
        if purged == True and np == True:
            continue
        elif deleted == True and nd == True:
            continue
        else:
            histories.append({'id':id, 'name':name, 'user__email':user__email, \
            'datasetnames':datasetnames, 'tags':tags, 'deleted':deleted, \
            'purged':purged, 'importing':importing, 'genome_build':\
            genome_build, 'published':published})
            CLEAN_NUM_HIST = CLEAN_NUM_HIST + 1

    return historiesRoot, NUM_HIST, CLEAN_NUM_HIST



def retrieve_historyDatasetAssociation(nd, np):
    """
    Retrieve historyDatasetAssociation
    """
    historyDatasetAssociation = []
    historyDatasetAssociationRoot = {'historyDatasetAssociation':\
    historyDatasetAssociation}

    NUM_HDA = sa_session.query(HistoryDatasetAssociation).count()

    ## HistoryDatasetAssociation
    all_historyDatasetAssociation = sa_session.query(HistoryDatasetAssociation).\
    filter_by(purged='False').order_by(HistoryDatasetAssociation.id)
    for hda in all_historyDatasetAssociation:
        if hda.blurb != "empty" and hda.blurb != "error" and \
        hda.blurb != "tool error" and hda.blurb != "queued" and \
        hda.blurb != "deleted" and hda.blurb != '0 bytes' and \
        hda.blurb is not None:
            try:
                hdadict = hda.to_dict()
            except Exception, e:
                # for debug. Could happen for files which are in database
                # (without the size), are not purged, but are no more
                # present on the disk.
                # However with previous filter on blurb and purged this
                # should not happen
                print "Can not convert this HDA object (id %d) into"  %(hda.id)+\
                " dictionnary because of the following error: %s " %(e)
            historyDatasetAssociation.append(hdadict)

    return historyDatasetAssociationRoot, NUM_HDA



#~ def retrieve_historyAnnotationAssociation(nd, np):
    #~ """
    #~ Retrieve historyAnnotationAssociation
    #~ """
    #~ historyAnnotationAssociation = []
    #~ historyAnnotationAssociationRoot = {'historyAnnotationAssociation':\
    #~ historyAnnotationAssociation}
#~
    #~ ## HistoryAnnotationAssociation
    #~ all_historyAnnotationAssociation = sa_session.query(HistoryAnnotationAssociation).\
    #~ all()
#~
    #~ return historyAnnotationAssociationRoot
#~
#~
#~
#~ def retrieve_historyDatasetAssociationAnnotationAssociation(nd, np):
    #~ """
    #~ Retrieve historyDatasetAssociationAnnotationAssociation
    #~ """
    #~ historyDatasetAssociationAnnotationAssociation = []
    #~ historyDatasetAssociationAnnotationAssociationRoot = \
    #~ {'historyDatasetAssociationAnnotationAssociation':\
    #~ historyDatasetAssociationAnnotationAssociation}
#~
    #~ ## HistoryDatasetAssociationAnnotationAssociation
    #~ all_historyDatasetAssociationAnnotationAssociation = sa_session.\
    #~ query(HistoryDatasetAssociationAnnotationAssociation).all()
#~
    #~ return historyDatasetAssociationAnnotationAssociationRoot



def retrieve_historyDatasetCollectionAssociation(nd, np):
    """
    Retrieve historyDatasetCollectionAssociation
    """
    historyDatasetCollectionAssociation = []
    historyDatasetCollectionAssociationRoot = {\
    'historyDatasetCollectionAssociation':historyDatasetCollectionAssociation}

    NUM_HDCA = sa_session.query(HistoryDatasetCollectionAssociation).count()

    ## HistoryDatasetCollectionAssociation
    all_historyDatasetCollectionAssociation = sa_session.\
    query(HistoryDatasetCollectionAssociation).all()

    for hdca in all_historyDatasetCollectionAssociation:
        hdcadict = hdca.to_dict()
        historyDatasetCollectionAssociation.append(hdcadict)
        #~ print hdcadict

    return historyDatasetCollectionAssociationRoot, NUM_HDCA


def retrieve_libraries(nd, np):
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
            libdict['root_folder__id'] = lib.root_folder.id
            libdict['root_folder__name'] = lib.root_folder.name
            libdict['root_folder__description'] = lib.root_folder.description
            libdict['root_folder__item_count'] = lib.root_folder.item_count
            libdict['root_folder__order_id'] = lib.root_folder.order_id
            libdict['root_folder__genome_build'] = lib.root_folder.genome_build
        libraries.append(libdict)

    return librariesRoot, NUM_LIB



def retrieve_libraryDatasetDatasetAssociations(nd, np):
    """
    Retrieve LibraryDataset objects
    """
    libraryDatasetDatasetAssociations = []
    libraryDatasetDatasetAssociationRoot = {\
    'libraryDatasetDatasetAssociations':libraryDatasetDatasetAssociations}

    NUM_LDDA = sa_session.query(LibraryDatasetDatasetAssociation).count()

    ## LibraryDatasetDatasetAssociation
    all_LibraryDatasetDatasetAssociation = sa_session.\
    query(LibraryDatasetDatasetAssociation).filter_by(deleted='False')
    for ld in all_LibraryDatasetDatasetAssociation:
        lddict = ld.to_dict()
        libraryDatasetDatasetAssociations.append(lddict)

    return libraryDatasetDatasetAssociationRoot, NUM_LDDA



def retrieve_libraryDatasets(nd, np):
    """
    Retrieve LibraryDataset objects
    """
    libraryDatasets = []
    libraryDatasetsRoot = {'libraryDatasets':libraryDatasets}

    NUM_LD = sa_session.query(LibraryDataset).count()

    ## LibraryDataset
    all_LibraryDatasets = sa_session.query(LibraryDataset).\
    filter_by(deleted='False')
    for ld in all_LibraryDatasets:
        lddict = ld.to_dict()
        if hasattr(ld, 'order_id'):
            lddict['order_id'] = ld.order_id
        libraryDatasets.append(lddict)

    return libraryDatasetsRoot, NUM_LD



def retrieve_libraryFolders(nd, np):
    """
    Retrieve LibraryFolders objects
    """
    libraryFolders = []
    libraryFoldersRoot = {'libraryFolders':libraryFolders}

    NUM_LF = sa_session.query(LibraryFolder).count()

    ## LibraryFolder
    all_libraryFolders = sa_session.query(LibraryFolder).all()
    for lf in all_libraryFolders:
        update_time = lf.update_time.isoformat()
        lfdict = {'name':lf.name, 'id':lf.id, 'item_count':lf.item_count, \
        'order_id':lf.order_id, 'description':lf.description, \
        'genome_build':lf.genome_build, 'update_time':update_time, \
        'parent_id':lf.parent_id}
        libraryFolders.append(lfdict)

    return libraryFoldersRoot, NUM_LF



def retrieve_libraryPermissions(nd, np):
    """
    Retrieve LibraryPermissions objects
    """
    libraryPermissions = []
    libraryPermissionsRoot = {'libraryPermissions':libraryPermissions}

    ## libraryPermissions
    all_libraryPermissions = sa_session.query(LibraryPermissions).all()
    for lp in all_libraryPermissions:
        library__name = lp.library.name
        library__id = lp.library.id
        role__name = lp.role.name
        action = lp.action
        lpdict = {'library__name':library__name, 'library__id':library__id, \
        'role__name':role__name, 'action':action}
        libraryPermissions.append(lpdict)

    return libraryPermissionsRoot



def retrieve_libraryFolderPermissions(nd, np):
    """
    Retrieve LibraryFolderPermissions objects
    """
    libraryFolderPermissions = []
    libraryFolderPermissionsRoot = {'libraryFolderPermissions':\
    libraryFolderPermissions}

    ## libraryFolderPermissions
    all_libraryFolderPermissions = sa_session.\
    query(LibraryFolderPermissions).all()
    for lfp in all_libraryFolderPermissions:
        folder__name = lfp.folder.name
        role__name = lfp.role.name
        action = lfp.action
        lfpdict = {'folder__name':folder__name, 'role__name':role__name, \
        'action':action}
        libraryFolderPermissions.append(lfpdict)

    return libraryFolderPermissionsRoot



def retrieve_libraryDatasetPermissions(nd, np):
    """
    Retrieve LibraryDatasetPermissions objects
    """
    libraryDatasetPermissions = []
    libraryDatasetPermissionsRoot = {'libraryDatasetPermissions':\
    libraryDatasetPermissions}

    ## libraryDatasetPermissions
    all_libraryDatasetPermissions = sa_session.\
    query(LibraryDatasetPermissions).all()
    for ldp in all_libraryDatasetPermissions:
        library_dataset__name = ldp.library_dataset.name
        role__name = ldp.role.name
        action = ldp.action
        ldpdict = {'library_dataset__name':library_dataset__name, \
        'role__name':role__name, 'action':action}
        libraryDatasetPermissions.append(ldpdict)

    return libraryDatasetPermissionsRoot



def retrieve_libraryDatasetDatasetAssociationPermissions(nd, np):
    """
    Retrieve LibraryDatasetDatasetAssociationPermissions objects
    """
    libraryDatasetDatasetAssociationPermissions = []
    libraryDatasetDatasetAssociationPermissionsRoot = \
    {'libraryDatasetDatasetAssociationPermissions':\
    libraryDatasetDatasetAssociationPermissions}

    ## libraryDatasetDatasetAssociationPermissions
    all_libraryDatasetDatasetAssociationPermissions = sa_session.query(\
    LibraryDatasetDatasetAssociationPermissions).all()
    for lddap in all_libraryDatasetDatasetAssociationPermissions:
        library_dataset_dataset_association__name = lddap.\
        library_dataset_dataset_association.name
        role__name = lddap.role.name
        action = lddap.action
        lddapdict = {'library_dataset_dataset_association__name':\
        library_dataset_dataset_association__name, 'role__name':role__name, \
        'action':action}
        libraryDatasetDatasetAssociationPermissions.append(lddapdict)

    return libraryDatasetDatasetAssociationPermissionsRoot


def retrieve_users(nd, np):
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
            users.append({'id':id, 'email':email, 'username':username, \
            'hashpassword':hashpassword, 'external':external, 'deleted':\
            deleted, 'purged':purged, 'active':active, 'activation_token':\
            activation_token, 'histories_names':histories_names, \
            'histories_ids':histories_ids})
            CLEAN_NUM_USERS = CLEAN_NUM_USERS + 1

    return usersRoot, NUM_USERS, CLEAN_NUM_USERS



def retrieve_workflows(nd, np):
    """
    Retrieve workflows with all steps
    """
    workflows = []
    workflowsRoot = {'workflows':workflows}

    NUM_WF = sa_session.query(Workflow).count()

    ## storedWorkflows
    all_stored_workflows = sa_session.query(StoredWorkflow).\
    order_by(StoredWorkflow.id)
    for swf in all_stored_workflows:
        swfdict = swf.to_dict()
        if hasattr(swf, 'latest_workflow_id'):
            swfdict['latest_workflow_id'] = swf.latest_workflow_id
        if hasattr(swf, 'slug'):
            swfdict['slug'] = swf.slug
        if hasattr(swf, 'user'):
            if swf.user is not None:
                swfdict['user__email'] = swf.user.email
        #~ if hasattr(swf, 'workflows'):
            #~ print(repr(swf.workflows))
        workflows.append(swfdict)

    ## workflow objects
    all_workflows = sa_session.query(Workflow).order_by(Workflow.name)
    for wf in all_workflows:
        wfdict = wf.to_dict()
        if hasattr(wf, 'user'):
            if wf.user is not None:
                wfdict['user__email'] = wf.user.email
        if hasattr(wf, 'uuid'):
            wfdict['uuid'] = wf.uuid
        wfdict['wst_id'] = []
        if hasattr(wf, 'steps'):
            for wst in wf.steps:
                wfdict['wst_id'].append(wst.id)
        workflows.append(wfdict)

    ## steps
    all_workflows_steps = sa_session.query(WorkflowStep).\
    order_by(WorkflowStep.id)
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
        wfs.append({'id':wfs_id, 'type':wfs_type, 'workflow_id':workflow_id, \
        'model_class':'WorkflowStep', 'tool_id':wfs_tool_id, \
        'tool_inputs':wfs_tool_inputs, 'tool_errors':wfs_tool_errors, \
        'position':wfs_position, 'input_connections':wfs_input_connections, \
        'config':wfs_config})
    workflows.append(wfs)

    ## output objects
    all_WorkflowOutput = sa_session.query(WorkflowOutput).\
    order_by(WorkflowOutput.output_name)
    for wfo in all_WorkflowOutput:
        workflows.append({'output_name':wfo.output_name, \
        'model_class':'WorkflowOutput', 'workflow_step_id':wfo.\
        workflow_step.id})

    ## invocations
    #~ all_WorkflowInvocation = sa_session.query(WorkflowInvocation).\
    #~order_by(WorkflowInvocation.id)
    #~ for wfi in all_WorkflowInvocation:
        #~ workflows.append(wfi.to_dict())
    ## invocations step
    #~ all_WorkflowInvocationStep = sa_session.query(WorkflowInvocationStep).\
    #order_by(WorkflowInvocationStep.id)
    #~ for wfis in all_WorkflowInvocationStep:
        #~ workflows.append(wfis.to_dict())

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
    type=argparse.FileType('wb', 0), help="JSON filename to create")
    parser.add_argument('-np', '--nopurged', action='store_true', \
    help="Do not backup purged elements")
    parser.add_argument('-nd', '--nodeleted', action='store_true', \
    help="Do not backup deleted elements")
    parser.add_argument('-v', '--verbose', action='store_true', \
    help="Display many informations except json datas if you choose output file [-o]")
    parser.add_argument('-b', '--backup', choices=['users', 'workflows', \
    'libraries', 'histories', 'datasets', 'all'], help="The data to backup", \
    required=True)

    args = parser.parse_args()

    backup2extract = args.backup

    try:
        if args.outfile:
            outfile = args.outfile
    except:
        outfile = False

    if args.nopurged:
        np = args.nopurged

    if args.nodeleted:
        nd = args.nodeleted

    if args.verbose:
        verbose = args.verbose

    backup = []

    if backup2extract == "users" or backup2extract == "all":
        users, num_users, clean_num_users = retrieve_users(nd, np)
        api_keys, num_keys = retrieve_apikeys(nd, np)
        roles, num_roles = retrieve_roles(nd, np)
        groups, num_groups = retrieve_groups(nd, np)
        associations = retrieve_associations()
        if verbose:
            print("\n####################################\n")
            print("%s USERS RETRIEVED" %(num_users))
            print("%s CLEAN USERS PROCESSED" %(clean_num_users))
            print("%s API KEYS RETRIEVED" %(num_keys))
            print("%s ROLES RETRIEVED" %(num_roles))
            print("%s GROUPS RETRIEVED" %(num_groups))
        backup.append(users)
        backup.append(api_keys)
        backup.append(roles)
        backup.append(groups)
        backup.append(associations)

    if backup2extract == "histories" or backup2extract == "all":
        histories, num_hist, clean_num_hist = retrieve_histories(nd, np)
        historyDatasetAssociation, num_hda = \
        retrieve_historyDatasetAssociation(nd, np)
        #historyAnnotationAssociation = \
        #retrieve_historyAnnotationAssociation(nd, np)
        #historyDatasetAssociationAnnotationAssociation = \
        #retrieve_historyDatasetAssociationAnnotationAssociation(nd, np)
        historyDatasetCollectionAssociation, num_hdca = \
        retrieve_historyDatasetCollectionAssociation(nd, np)
        if verbose:
            print("\n####################################\n")
            print("%s HISTORIES RETRIEVED" %(num_hist))
            print("%s CLEAN HISTORIES PROCESSED" %(clean_num_hist))
            print("%s HISTORIES DATASETS ASSOCIATIONS RETRIEVED" %(num_hda))
            print("%s HISTORIES DATASETS COLLECTIONS ASSOCIATIONS RETRIEVED" \
            %(num_hdca))
        backup.append(histories)
        backup.append(historyDatasetAssociation)
        #backup.append(historyAnnotationAssociation)
        #backup.append(historyDatasetAssociationAnnotationAssociation)
        backup.append(historyDatasetCollectionAssociation)

    if backup2extract == "datasets" or backup2extract == "all":
        datasets, num_dat, clean_num_dat = retrieve_datasets(nd, np)
        datasetPermissions = retrieve_datasetPermissions(nd, np)
        if verbose:
            print("\n####################################\n")
            print("%s DATASETS RETRIEVED" %(num_dat))
            print("%s CLEAN DATASETS PROCESSED" %(clean_num_dat))
        backup.append(datasets)
        backup.append(datasetPermissions)

    if backup2extract == "workflows" or backup2extract == "all":
        workflows, num_wf = retrieve_workflows(nd, np)
        if verbose:
            print("\n####################################\n")
            print("%s WORKFLOWS RETRIEVED" %(num_wf))
        backup.append(workflows)

    if backup2extract == "libraries" or backup2extract == "all":
        libraries, num_lib = retrieve_libraries(nd, np)
        libraryDatasets, num_ld = retrieve_libraryDatasets(nd, np)
        libraryDatasetDatasetAssociations, num_ldda = \
        retrieve_libraryDatasetDatasetAssociations(nd, np)
        libraryFolders, num_lf = retrieve_libraryFolders(nd, np)
        libraryPermissions = retrieve_libraryPermissions(nd, np)
        libraryFolderPermissions = retrieve_libraryFolderPermissions(nd, np)
        libraryDatasetPermissions = retrieve_libraryDatasetPermissions(nd, np)
        libraryDatasetDatasetAssociationPermissions = \
        retrieve_libraryDatasetDatasetAssociationPermissions(nd, np)
        if verbose:
            print("\n####################################\n")
            print("%s LIBRARIES RETRIEVED" %(num_lib))
            print("%s LIB_DATASETS RETRIEVED" %(num_ld))
            print("%s LIB_DATASET_ASSOCIATIONS RETRIEVED" %(num_ldda))
            print("%s LIB_FOLDERS RETRIEVED" %(num_lf))
        backup.append(libraryFolders)
        backup.append(libraries)
        backup.append(libraryDatasets)
        backup.append(libraryDatasetDatasetAssociations)
        backup.append(libraryPermissions)
        backup.append(libraryFolderPermissions)
        backup.append(libraryDatasetPermissions)
        backup.append(libraryDatasetDatasetAssociationPermissions)


    backup = dumps(backup, default=decimal_default, sort_keys=True, indent=4)

    if not outfile:
        print(backup)
    else:
        outfile.write(backup)
        outfile.close()
