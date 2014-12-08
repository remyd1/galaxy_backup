#!/usr/bin/env python

"""
This code load a json file and then recreate all the galaxy metadatas.

You shoud use it in the right order. That means that you can import an history,
even if there is no user and no dataset, but it will fail or it will be empty...

Right order:
    - users
    - histories
    - libraries
    - datasets
    - workflows
"""

"""
TODO :
    encoding unicode to ascii / normalize (remove special character)
"""

import sys
import os
import argparse
import datetime

from json import loads

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


def check_python_version():
    if sys.version_info < (2, 7, 0):
        sys.exit("You need python 2.7 (because of argparse) or later to run this script\n")



# begin of the interesting part here
from scripts.db_shell import *



def getjson(infile):
    """
    Retrieve galaxy datas from json
    """
    try:
        data = infile.read()
        infile.close()
        #if not limited to 4500 characters ?
        try:
            jsondata = json.loads(data)
            return jsondata
        except:
            sys.exit("Not a json file !! Bye bye...")
    except Exception as e:
        top = traceback.extract_stack()[-1]
        print ', '.join([type(e).__name__, os.path.basename(top[0]), str(top[1])])
    #~ except:
        #~ sys.exit("Your file is not a text file readable !! Bye "+\
        #~ "bye...")



def parse_json_data(jsondata, restore_purged, restore_deleted, verbose):
    """
    Parse json and call the appropriate code to generate elements
    """
    for type_of_backup in jsondata:
        if type_of_backup.has_key('users'):
            users = type_of_backup['users']
            create_users(users, restore_purged, restore_deleted, verbose)
        elif type_of_backup.has_key('histories'):
            histories = type_of_backup['histories']
            create_histories(histories, restore_purged, restore_deleted, verbose)
        elif type_of_backup.has_key('workflows'):
            workflows = type_of_backup['workflows']
            create_workflows(workflows, restore_purged, restore_deleted, verbose)
        elif type_of_backup.has_key('datasets'):
            datasets = type_of_backup['datasets']
            create_datasets(datasets, restore_purged, restore_deleted, verbose)
        elif type_of_backup.has_key('libraryFolders'):
            libraryFolders = type_of_backup['libraryFolders']
            create_libraryFolders(libraryFolders, restore_purged, restore_deleted, verbose)
        elif type_of_backup.has_key('libraries'):
            libraries = type_of_backup['libraries']
            create_libraries(libraries, restore_purged, restore_deleted, verbose)
        elif type_of_backup.has_key('libraryDatasets'):
            libraryDatasets = type_of_backup['libraryDatasets']
            create_libraryDatasets(libraryDatasets, restore_purged, restore_deleted, verbose)
        elif type_of_backup.has_key('libraryDatasetDatasetAssociations'):
            ldda = type_of_backup['libraryDatasetDatasetAssociations']
            create_libraryDatasetDatasetAssociations(ldda, restore_purged, restore_deleted, verbose)



def create_users(users, restore_purged, restore_deleted, verbose):
    """
    Create a new user with email/hash password, username...
    """
    if verbose:
        print("\n ####### USERS #######")
    for user in users:
        if verbose:
            print("A new user has been discovered: %s" %( user['email']) )
        # check if this user already exists
        user_e = sa_session.query(User).filter_by(email=user['email']).count()
        if user_e == 0:
            new_user = User( user['email'], user['hashpassword'] )
            new_user.username = user['username']
            new_user.external = user['external']
            new_user.activation_token = user['activation_token']
            new_user.deleted = user['deleted']
            new_user.purged = user['purged']
            new_user.active = user['active']
            if user['deleted'] is False and user['purged'] is False:
                sa_session.add( new_user )
            elif restore_purged is True and user['purged'] is True:
                sa_session.add( new_user )
            elif restore_deleted is True and user['deleted'] is True:
                sa_session.add( new_user )
            sa_session.flush()
        else:
            if verbose:
                print( "This user already exists %s !" %( user['email'] ) )



def create_histories(histories, restore_purged, restore_deleted, verbose):
    """
    Create a new history associated to an existing user
    (empty if there is no dataset)
    """
    if verbose:
        print("\n ####### HISTORIES #######")
    for history in histories:
        if verbose:
            print("A new history has been discovered: %s" %( history['name']) )
        try:
            the_owner = sa_session.query(User).filter_by(email=history['email']).one()
        except (MultipleResultsFound, NoResultFound) as e:
            print("You have an error when trying to retrieving the owner of " + \
            " this history (%s)" % ( e ) )
            continue
        ## retrieve history if it exists
        history_e = sa_session.query(History).filter(History.name == history['name']).\
        filter(User.email == history['email']).count()
        if history_e == 0:
            ## transform back that dict to an History object with a new
            ## generated id to avoid any id overwritten
            new_history = History( None, history['name'], the_owner )
            new_history.tags = history['tags']
            new_history.deleted = history['deleted']
            new_history.purged = history['purged']
            new_history.importing = history['importing']
            new_history.genome_build = history['genome_build']
            new_history.published = history['published']
            new_history.datasets = []
            for dataset_name in history['datasetnames']:
                try:
                    new_history.datasets.append(sa_session.query(Dataset).\
                    filter_by(external_filename=dataset_name).one())
                except:
                    # dataset not found (does not exist yet)
                    pass
            if history['deleted'] is False and history['purged'] is False:
                sa_session.add( new_history )
                ## perhaps, a better choice would be to use 'History' copy method, like that:
                # new_history.copy( history['name'], history['user'], True, False )
            elif restore_deleted is True and history['deleted'] is True:
                sa_session.add( new_history )
            elif restore_purged is True and history['purged'] is True:
                sa_session.add( new_history )

            sa_session.flush()
        else:
            if verbose:
                print( "This History seems to already exists '%s' (%s) !" \
                %(history['name'], history['email']) )



def create_workflows(workflows, restore_purged, restore_deleted, verbose):
    """
    Create workflows with all steps and keep associated users
    """
    if verbose:
        print("\n ####### WORKFLOWS #######")
    for workflow in workflows:
        if type(workflow) is list:
            # 'model_class' == 'WorkflowStep' for each item
            for wfs in workflow:
                the_workflowStep = None
                the_workflow = None
                addingStep = False
                if verbose:
                    print("Processing workflow step: %s" %( wfs['id'] ))
                ## normally, the step already exists because we process
                ## workflow first.
                the_workflowStep = sa_session.query(WorkflowStep).\
                get(wfs['id'])
                if not the_workflowStep:
                    the_workflowStep = WorkflowStep()
                    addingStep = True
                the_workflowStep.config = wfs['config']
                the_workflowStep.id = wfs['id']
                the_workflow = sa_session.query(Workflow).get(wfs['workflow_id'])
                if the_workflow:
                    the_workflowStep.Workflow = the_workflow
                the_workflowStep.workflow_id = wfs['workflow_id']
                the_workflowStep.position = wfs['position']
                the_workflowStep.tool_errors = wfs['tool_errors']
                the_workflowStep.tool_inputs = wfs['tool_inputs']
                the_workflowStep.type = wfs['type']
                the_workflowStep.input_connections = []
                if wfs.has_key("input_connections"):
                    for ic in wfs['input_connections']:
                        new_workflowIC = WorkflowStepConnection()
                        new_workflowIC.input_name = ic['input_name']
                        new_workflowIC.output_name = ic['output_name']
                        new_workflowIC.input_step_id = ic['input_step_id']
                        new_workflowIC.output_step_id = ic['output_step_id']
                        sa_session.add( new_workflowIC )
                        the_workflowStep.input_connections.append(new_workflowIC)
                if addingStep is True:
                    sa_session.add( the_workflowStep )
        elif type(workflow) is dict:
            if workflow['model_class'] == "Workflow":
                if verbose:
                    print("A new workflow has been discovered: %s" %( workflow['name'] ))

                if workflow.has_key('uuid'):
                    new_workflow = Workflow(workflow['uuid'])
                else:
                    new_workflow = Workflow()
                if workflow.has_key('user'):
                    # a Workflow must be linked to a user
                    new_workflow.user = workflow['user']
                    new_workflow.name = workflow['name']
                    new_workflow.has_cycles = workflow['has_cycles']
                    new_workflow.has_errors = workflow['has_errors']
                    new_workflow.steps = []
                    if workflow.has_key('wst_id'):
                        the_step = None
                        for st_id in workflow['wst_id']:
                            the_step = sa_session.query(WorkflowStep).\
                            get(st_id)
                            new_workflow.steps.append(the_step)
                            if not the_step:
                                # no step found. Need to add it
                                new_step = WorkflowStep()
                                new_step.id = st_id
                                sa_session.add( new_step )
                    ## creating a StoredWorkflow linked to this Workflow
                    ## or check if there is already an existing StoredWorkflow
                    try:
                        stored = sa_session.query(StoredWorkflow). \
                        filter(StoredWorkflow.name == workflow['name']). \
                        filter(StoredWorkflow.user == workflow['user']).one()
                    except:
                        stored = StoredWorkflow()
                    stored.name = new_workflow.name
                    new_workflow.stored_workflow = stored
                    stored.latest_workflow = new_workflow
                    sa_session.add( new_workflow )
            elif workflow['model_class'] == "StoredWorkflow":
                if verbose:
                    print("A new StoredWorkflow has been discovered: %s" %( workflow['name'] ))
                if restore_deleted is False and workflow['deleted'] is False:
                    continue
                else:
                    if workflow.has_key('user'):
                        # a (Stored)Workflow must be linked to a user
                        new_StoredWorkflow = StoredWorkflow()
                        new_StoredWorkflow.id = workflow['id']
                        new_StoredWorkflow.latest_workflow_id = workflow['latest_workflow_id']
                        new_StoredWorkflow.name = workflow['name']
                        new_StoredWorkflow.user.email = workflow['user_email']
                        new_StoredWorkflow.published = workflow['published']
                        new_StoredWorkflow.tags = workflow['tags']
                        sa_session.add( new_StoredWorkflow )
            elif workflow['model_class'] == "WorkflowOutput":
                if verbose:
                    print("Processing new workflow object: %s" %( workflow['output_name'] ))
                try:
                    the_step = sa_session.query(WorkflowStep). \
                    filter(WorkflowStep.id == workflow['workflow_step_id']).one()
                except:
                    # no step corresponding to this ID
                    continue
                new_WO = WorkflowOutput(the_step, workflow['output_name'])
                sa_session.add( new_WO )
            #~ elif workflow['model_class'] == "WorkflowInvocation":
                #~ pass
            #~ elif workflow['model_class'] == "WorkflowInvocationStep":
                #~ pass
        sa_session.flush()



def create_datasets(datasets, restore_purged, restore_deleted, verbose):
    """
    Create datasets
    """
    if verbose:
        print("\n ####### DATASETS #######")
    for dataset in datasets:
        if verbose:
            print("A new dataset has been discovered; id: %s" %( dataset['id']) )
        new_dataset = Dataset()
        new_dataset.id = dataset['id']
        new_dataset.state = dataset['state']
        new_dataset.deleted = dataset['deleted']
        new_dataset.purged = dataset['purged']
        new_dataset.external_filename = dataset['external_filename']
        new_dataset.purgable = dataset['purgable']
        new_dataset.file_size = dataset['file_size']
        new_dataset.extra_files_path = dataset['extra_files_path']
        new_dataset.external_extra_files_path = dataset['external_extra_files_path']
        if restore_deleted is True and dataset['deleted'] is True:
            sa_session.add(new_dataset)
        elif restore_purged is True and dataset['purged'] is True:
            sa_session.add(new_dataset)
        elif dataset['purged'] is False and dataset['deleted'] is False:
            sa_session.add(new_dataset)
        sa_session.flush()


def create_libraries(libraries, restore_purged, restore_deleted, verbose):
    """
    Create libraries
    """
    if verbose:
        print("\n ####### LIBRARIES #######")
    for library in libraries:
        if verbose:
            print("A new librarie has been discovered: %s" %( library['name']) )

        # check if this library already exists
        library_e = sa_session.query(Library).filter(Library.name == library['name']).\
        filter(Library.id == library['id']).count()
        library_e_id = sa_session.query(Library).get(library['id'])
        if library_e == 0:
            new_library = Library()
            new_library.name = library['name']
            new_library.description = library['description']
            new_library.synopsis = library['synopsis']
            library['deleted'] = False
            if library.has_key('deleted'):
                new_library.deleted = library['deleted']
            if not library_e_id:
                new_library.id = library['id']
            if library.has_key('root_folder_id'):
                # check if root_folder already exists (must be imported before)
                the_lf = sa_session.query(LibraryFolder).get(ld['root_folder_id'])
                if the_lf:
                    new_library.root_folder = the_lf
                else:
                    new_libFolder = LibraryFolder()
                    new_libFolder.id = library['root_folder_id']
                    new_libFolder.name = library['root_folder_name']
                    new_libFolder.description = library['root_folder_description']
                    new_libFolder.item_count = library['root_folder_item_count']
                    new_libFolder.order_id = library['root_folder_order_id']
                    new_libFolder.genome_build = library['root_folder_genome_build']
                    sa_session.add( new_libFolder )
            if restore_deleted is True and library['deleted'] is True:
                sa_session.add(new_library)
            elif library['deleted'] is False:
                sa_session.add(new_library)
            sa_session.flush()


def create_libraryDatasets(libraryDatasets, restore_purged, restore_deleted, verbose):
    """
    Create LibraryDataset objects
    """
    if verbose:
        print("\n ####### libraryDatasets #######")

    # retrieve all LibraryDataset(s)
    all_current_ld = sa_session.query(LibraryDataset).all()
    present = False

    for ld in libraryDatasets:
        if verbose:
            print("A new libraryDataset has been discovered: %s" %( ld['name']) )
        #~ name = ld['name'].encode('ascii', 'ignore')

        for the_ld in all_current_ld:
                if ld['name'] == the_ld.name and ld['misc_info'] == the_ld.info:
                    if verbose:
                        print("An existing LibraryDataset seems to already exists")
                    present = True
        ld_e_id = sa_session.query(LibraryDataset).get(ld['id'])
        the_lf = sa_session.query(LibraryFolder).get(ld['folder_id'])
        if present is False:
            new_ld = LibraryDataset()
            new_ld.name = ld['name']
            new_ld.info = ld['misc_info']
            new_ld.blurb = ld['misc_blurb']
            if ld.has_key('peek'):
                new_ld.peek = ld['peek']
            new_ld.dbkey = ld['metadata_dbkey']
            if the_lf:
                new_ld.folder = the_lf
            new_ld.parent_id = ld['parent_library_id']
            new_ld.genome_build = ld['genome_build']
            new_ld.date_uploaded = ld['date_uploaded']
            new_ld.state = ld['state']
            if ld.has_key('deleted'):
                new_ld.deleted = ld['deleted']
            else:
                ld['deleted'] = False
            if not ld_e_id:
                new_ld.id = ld['id']
            if restore_deleted is True and ld['deleted'] is True:
                sa_session.add(new_ld)
            elif ld['deleted'] is False:
                sa_session.add(new_ld)
            sa_session.flush()


def create_libraryDatasetDatasetAssociations(ldda, restore_purged, restore_deleted, verbose):
    """
    Create LibraryDatasetDatasetAssociation objects
    """
    if verbose:
        print("\n ####### libraryDatasetDatasetAssociations #######")
    for the_ldda in ldda:
        if verbose:
            print("A new LibraryDatasetDatasetAssociation has been discovered: %s" %( the_ldda['name']) )

        # check if this library already exists
        the_ldda_e = sa_session.query(LibraryDatasetDatasetAssociation).\
        filter(LibraryDatasetDatasetAssociation.name == the_ldda['name']).\
        filter(LibraryDatasetDatasetAssociation.id == the_ldda['id']).count()
        the_ldda_e_id = sa_session.query(LibraryDatasetDatasetAssociation).\
        get(the_ldda['id'])
        if the_ldda_e == 0:
            new_ldda = LibraryDatasetDatasetAssociation()
            new_ldda.name = the_ldda['name']
            new_ldda.info = the_ldda['misc_info']
            new_ldda.blurb = the_ldda['misc_blurb']
            if the_ldda.has_key('peek'):
                new_ldda.peek = the_ldda['peek']
            new_ldda.dbkey = the_ldda['metadata_dbkey']
            new_ldda.extension = the_ldda['file_name'].rpartition(".")[-1]
            # retrieving corresponding dataset if it exists
            try:
                the_dataset_e = sa_session.query(LibraryDatasetDatasetAssociation).\
                filter(dataset.external_filename == the_ldda['file_name']).one()
                new_ldda.dataset = the_dataset_e
            except:
                pass
            if the_ldda.has_key('library_dataset_id'):
                the_ld = sa_session.query(LibraryDataset).\
                get(the_ldda['library_dataset_id'])
                if the_ld:
                    new_ldda.library_dataset = the_ld
            #~ copied_from_history_dataset_association = new_ldda
            #~ new_ldda.user = the_ldda['uuid']
            new_ldda.parent_id = the_ldda['parent_library_id']
            new_ldda.description = the_ldda['description']
            new_ldda.update_time = the_ldda['update_time']
            new_ldda.genome_build = the_ldda['genome_build']
            new_ldda.visible = the_ldda['visible']
            if the_ldda.has_key('deleted'):
                new_ldda.deleted = the_ldda['deleted']
            else:
                the_ldda['deleted'] = False
            if not ldda_e_id:
                new_ldda.id = the_ldda['id']
            if restore_deleted is True and the_ldda['deleted'] is True:
                sa_session.add(new_ldda)
            elif the_ldda['deleted'] is False:
                sa_session.add(new_ldda)
            sa_session.flush()


def create_libraryFolders(libraryFolders, restore_purged, restore_deleted, verbose):
    """
    Create LibraryFolder objects
    """
    if verbose:
        print("\n ####### libraryFolders #######")
    for lf in libraryFolders:
        if verbose:
            print("A new LibraryFolder has been discovered: %s" %( lf['name']) )

        # check if this library already exists
        lf_e = sa_session.query(LibraryFolder).filter(LibraryFolder.name == lf['name']).\
        filter(LibraryFolder.id == lf['id']).count()
        lf_e_id = sa_session.query(LibraryFolder).get(lf['id'])
        if lf_e == 0:
            new_lf = LibraryFolder()
            new_lf.name = lf['name']
            new_lf.description = lf['description']
            new_lf.genome_build = lf['genome_build']
            new_lf.item_count = lf['item_count']
            new_lf.order_id = lf['order_id']
            new_lf.parent_id = lf['parent_id']
            new_lf.update_time = datetime.datetime.strptime( lf['update_time'] , \
            "%Y-%m-%d %H:%M:%S.%f" )
            if lf.has_key('deleted'):
                new_lf.deleted = lf['deleted']
            else:
                lf['deleted'] = False
            if not lf_e_id:
                # could be an issue if a new id is enerated
                # (parent_id / relation with LibraryDataset...)
                new_lf.id = lf['id']
            if restore_deleted is True and lf['deleted'] is True:
                sa_session.add(new_lf)
            elif lf['deleted'] is False:
                sa_session.add(new_lf)
            sa_session.flush()



if __name__ == '__main__':
    """
    Main code to retrieve datas from the backup you need.
    < Takes a JSON file to migrate the data in the new galaxy DB
    > Returns an import status for each element
    """

    restore_purged = True
    restore_deleted = True
    verbose = False

    check_python_version()

    usage = "usage :\n\
    \n\
    cd /path/to/galaxy \n\n\
    && \n\
    python import_script.py -i --infile /path/to/filename\n\n\
    -help\n"
    epilog = "galaxy import_script. A beta"\
    + " release from Remy Dernat, Evolution Science Institute -"\
    + " Montpellier, France."

    parser = argparse.ArgumentParser(description=usage, epilog=epilog)
    parser.add_argument('--version', action='version', version='%(prog)s 0.1')

    parser.add_argument('-i', '--infile', required=True, \
    type=argparse.FileType('r'), help = "JSON file to import created"+ \
    " with backup_script.py")
    parser.add_argument('-igp', '--ignore_purged', action='store_true', \
    help = "Ignore purged elements if found any in json file")
    parser.add_argument('-igd', '--ignore_deleted', action='store_true', \
    help = "Ignore deleted elements if found any in json file")
    parser.add_argument('-v', '--verbose', action='store_true', \
    help = "Display many informations")

    args = parser.parse_args()

    infile = args.infile

    if(args.ignore_deleted):
        restore_deleted = False

    if(args.ignore_purged):
        restore_purged = False

    if(args.verbose):
        verbose = True

    if infile.name.rpartition(".")[-1] != "json":
        is_json_file = raw_input("Are you sure that this file is a json file ? [y]/n:")
        if is_json_file is "Y" or is_json_file is "y" or not is_json_file:
            print("Ok, I will try to process this file.")
        else:
            sys.exit("Ok. So, try again!\n")


    jsondata = getjson(infile)

    parse_json_data(jsondata, restore_deleted, restore_purged, verbose)


