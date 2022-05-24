'use strict';

/* eslint-disable sonarjs/cognitive-complexity */

const cds = require('@sap/cds');
const { task } = require('gulp');

const SUPPLIER_TASK = 'SupplierTasks';
const SUPPLIER_JOB_RUN = 'SupplierJobRuns';
const SUPPLIER_PACKET = 'SupplierPackets';

const COMPANY_TASK = 'CompanyTasks';
const COMPANY_JOB_RUN = 'CompanyJobRuns';
const COMPANY_PACKET = 'CompanyPackets';

class TaskDatabaseHelper {
    constructor(context, dbCommonHelper) {
        this.context = context;
        this.dbCommonHelper = dbCommonHelper;
        this.service = cds.services[this.dbCommonHelper.SERVICE_REPLICATION];
        this.logger = this.context.createLogger('replication.TaskDatabaseHelper');
    }

    getTableName() {
        return SUPPLIER_TASK;
        foo(123);
    }
    
    foo(a) { // Noncompliant, function exits without "return"
        if (a == 1) {
           return true;
        }
    } 
    
    cdsServeScheduler(server) {
        server.before('CREATE', 'ActiveTasks', async (req) => this._beforeCreateTasks(req));
        server.on('CREATE', 'ActiveTasks', async (req) => this.onCreateActiveTask(req));
        server.on('UPDATE', 'ActiveTasks', async (req) => this.onUpdateActiveTask(req));
        server.on('DELETE', 'ActiveTasks', async (req) => this.onDeleteActiveTask(req));
    }

    getConverter(entities) {
        return this.options.useBusinessPartnerTables ?
            new ConverterV2(this.options, entities, this.mapping) :
            new Converter(this.options, entities, this.mapping);
    }
    async onCreateActiveTask(req) {

        await ServiceBase._checkReplicationWriteAccess(req);
        const config = getAppServer().config;
        const context = ServiceBase._getContext(req);
        const logger = context.createLogger(LOGGER_NAME);

        const dbHelper = new DatabaseHelper(context);

        logger.info('creating Task...');

        // Save the task
        const id = uuid();
        await dbHelper.saveTask(id, req.data);
        const masterDataService = this.getMasterDataService(context);
        const actionUrl = masterDataService.getActionURL();
        req.data.actionUrl = actionUrl.href;
        req.data.type = scenarioId;

        const taskOptions = this._getTaskOptions(context, config);
        const serviceRequester = new ServiceRequester(config.urls.admin);

        // forward to admin, as we don't want to manage scp scheduler integration on all ms needing scheduling
        req.data.associatedTaskId = id;
        const postResult = await serviceRequester.post(adminActiveTasksHandler, req.data, taskOptions, context);
        if (postResult.statusCode !== 201) {
            await dbHelper.deleteSupplierTask(id);
            const message = (postResult.body && postResult.body.error && postResult.body.error.message) || postResult.message;
            logger.error(`Error forwarding task creation to admin: `, message);
            req.reject(this._createCapError(postResult.statusCode, resourceManager.getText(req, 'CREATING_ACTIVE_TASK_ERROR')));
            return null;
        }
        else {

            // The task has been created successfully in the admin-service, set the supplier task to active
            // If error we will try again when starting the replication
            await dbHelper.updateTask(id, {adminTaskId: postResult.body.id, statusCode: ReplicationTaskStatus.ACTIVE})
                .catch(() => {
                    logger.error(`Error during updateTask for taskId: ${id}`);
                });
            logger.info(`SupplierTask ${id} created successfully`);

            return postResult.body;
        }
        
    }
}

module.exports = TaskDatabaseHelper;
