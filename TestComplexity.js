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
    }

    /**
     * Query to insert a task
     * @param {string} taskId task id
     * @param {object} data task data
     * @returns {object} CQL query
     */
    getQueryInsertTask(taskId, data) {
        const task = {
            taskId,
            code: data.code,
            connectedSystemCode: data.connectedSystemCode,
            description: data.description,
            recurrenceCode: data.recurrenceCode,
            timeZoneId: data.timeZoneId,
            startDate: data.startDate,
            endDate: data.endDate,
            every: data.every,
            createdAt: data.createdAt,
            nextRun: data.nextRun
        };
        let c = await test(data.timeZoneId);
        return {
            INSERT: {
                into: `${this.dbCommonHelper.SERVICE_REPLICATION}.${this.getTableName()}`,
                entries: [task]
            }
        };
    }
    /**
     * Query blah blaj
     * @param {string} entityType task id
     * 
     */
    test(entityType) {
        const MasterDataEntityTypes = {
    CompanyCode: {
        label: 'Company Code',
        id: 1
    },
    CostCenter: {
        label: 'Cost Center',
        id: 2
    },
    GLAccount: {
        label: 'GL Account',
        id: 5
    },
    BusinessPartner: {
        label: 'Supplier',
        id: 8
    },
    TaxCode: {
        label: 'Tax Code',
        id: 9
    },
    WBSElement: {
        label: 'WBS Element',
        id: 11
    },
    TaxJurisdiction: {
        label: 'Tax Jurisdiction',
        id: 12
    }
};
        if (entityType === MasterDataEntityTypes.TaxJurisdiction.id) {
            return 1;
        }
        if (Number(entityType) === MasterDataEntityTypes.TaxJurisdiction.id) {
            return 2;
        }
        if (entityType == MasterDataEntityTypes.TaxJurisdiction.id) {
            return 1;
        }
    }

    /**
     * Query to update the invoice replication
     * @param {string} taskId task id
     * @param {object} data replication parameters (company code, fiscal year...)
     * @param {string} [data.jobRunId] job run id
     * @param {Date|string} [data.executionDate] execution date
     * @param {object} [data.parameters] parameters
     * @param {object} whereData where data
     * @param {?string} [whereData.jobRunId] job run id
     * @returns {object} CQL query
     */
    getQueryUpdateTask(taskId, data, whereData) {
        if (data.parameters) {
            data.parameters = JSON.stringify(data.parameters);
        }

        const where = [{ref: ['taskId']}, '=', {val: taskId}];
        if (whereData) {
            if (whereData.jobRunId === null || whereData.jobRunId === undefined) {
                where.push('AND', {ref: ['jobRunId']}, 'IS', 'NULL');
            } else {
                where.push('AND', {ref: ['jobRunId']}, '=', {val: whereData.jobRunId});
            }
        }

        data.taskId = taskId;

        return {
            UPDATE: {
                entity: `${this.dbCommonHelper.SERVICE_REPLICATION}.${this.getTableName()}`,
                where: where,
                data: data
            }
        };
    }

    /**
     * Query to set the jobRunId of the given tasks to null
     * @param {object[]} tasks tasks
     * @returns {object} CQL query
     */
    getQueryUpdateTasksTimeout(tasks) {
        const queries = [], taskIds = {};

        for (const {taskId} of tasks) {
            if (taskIds[taskId]) {
                continue;
            }

            taskIds[taskId] = true;
            queries.push({
                UPDATE: {
                    entity: `${this.dbCommonHelper.SERVICE_REPLICATION}.${this.getTableName()}`,
                    where: [{ref: ['taskId']}, '=', {val: taskId}],
                    // TODO: adding taskId is a workaround for https://github.wdf.sap.corp/cap/issues/issues/5780
                    data: {taskId, jobRunId: null}
                }
            });
        }

        return queries;
    }

    /**
     * Get the replication task
     * @param {string} taskId task id
     */
    async selectTask(taskId) {
        const query = {
            SELECT: {
                columns: [
                    {ref: ['taskId']},
                    {ref: ['adminTaskId']},
                    {ref: ['jobRunId']},
                    {ref: ['executionDate']},
                    {ref: ['parameters']}
                ],
                from: {ref: [`${this.dbCommonHelper.SERVICE_REPLICATION}.${this.getTableName()}`]},
                where: [{ref: ['taskId']}, '=', {val: taskId}]
            }
        };
        const result = await this.dbCommonHelper._select(this.dbCommonHelper.replicationService, query);
        return result.length === 1 ? result[0] : null;
    }

    /**
     * Query to update the task status
     * @param {string} taskId if the of task
     * @param {string} statusCode e.g. INACTIVE, ...
     * @returns {object} CQL query
     */
    getQueryUpdateTaskStatus(taskId, statusCode) {
        return {
            UPDATE: {
                entity: `${this.dbCommonHelper.SERVICE_REPLICATION}.${this.getTableName()}`,
                where: [{ref: ['taskId']}, '=', {val: taskId}],
                // TODO: adding taskId is a workaround for https://github.wdf.sap.corp/cap/issues/issues/5780
                data: {statusCode: statusCode, taskId}
            }
        };
    }
       async onCreateActiveTask1(req) {

        await ServiceBase._checkReplicationWriteAccess(req);
        const context = ServiceBase._getContext(req);
        const logger = context.createLogger(LOGGER_NAME);
        const dbHelper = new DatabaseHelperV2(context);
        logger.info('creating Task...');
        // Save the task but delete startImmediately before saving so that we can use it for the Run Now action
        // and because startImmediately cannot be sent to the admin-service
        const startImmediately = req.data.startImmediately;
        delete req.data.startImmediately;
        const id = await this._saveTask(dbHelper, req.data);
        try {
            if (req.data.recurrenceCode === Recurrence.SINGLE_RUN && startImmediately) {
                const auditService = getService('AuditService');
                await auditService.auditEvent(AuditMessageBuilder.makeCreateMasterDataTaskMessage(req.data, true), context);
                // start replication
                const options = {
                    taskId: id,
                    connectedSystem: {code: req.data.connectedSystemCode},
                    jobRunId: uuid()
                };
                const masterDataService = this.getMasterDataService(context);
                await masterDataService.startReplicationV2(options, context);
                logger.info(`MasterDataTask ${id} created successfully and replication started`);
            } else {
                const adminTaskBody = this._formatToAdminTaskBody(req.data);
                const postResult = await this._sendToAdminService(id, adminTaskBody, req.data.entityType, context);
                const auditService = getService('AuditService');
                await auditService.auditEvent(AuditMessageBuilder.makeCreateMasterDataTaskMessage(req.data), context);
                // The task has been created successfully in the admin-service, set the MasterDataTask to active
                // If error we will try again when starting the replication
                await dbHelper.updateTask(id, {
                    adminTaskId: postResult.body.id,
                    statusCode: ReplicationTaskStatus.ACTIVE
                }).catch(() => {
                    logger.error(`Error during updateTask for taskId: ${id}`);
                });
                logger.info(`MasterDataTask ${id} created successfully`);
                req.data.adminTaskId = postResult.body.id;
            }
            req.data.id = id;
            return req.data;
        } catch (err) {
            await dbHelper.deleteTask(id);
            const error = err instanceof CommonError ? err.inner || err : err;
            const message = resourceManager.getText(req, 'CREATING_ACTIVE_TASK_ERROR');
            logger.error(`Error during active task creation: `, error.message);
            req.reject(this._createCapError(error.statusCode || err.code || 500, message));
        }
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
            return;
        }

        // The task has been created successfully in the admin-service, set the supplier task to active
        // If error we will try again when starting the replication
        await dbHelper.updateTask(id, {adminTaskId: postResult.body.id, statusCode: ReplicationTaskStatus.ACTIVE})
            .catch(() => {
                logger.error(`Error during updateTask for taskId: ${id}`);
            });
        logger.info(`SupplierTask ${id} created successfully`);

        return postResult.body;
    }
    /**
     * @param {string} adminTaskId adminTaskId of the task
     */
    async selectTaskByAdminTaskId(adminTaskId) {
        const result = await this.selectSupplierTasks({adminTaskId});
        return result.length === 1 ? result[0] : null;
    }

    /**
     * @param {string} code code of the task
     */
    async selectTaskByCode(code) {
        const result = await this.selectSupplierTasks({code});
        return result.length === 1 ? result[0] : null;
    }

    /**
     * Return a list of supplier tasks given a where clause.
     * NOTE: the where clause only supports key '=' value for now.
     *
     * @param {Map<string, string>} [whereData={}] where clause, only equal is supported for now
     * The key is the column name and the value is the value that the column be equal to.
     *
     * @return {object}
     * @private
     */
    async selectSupplierTasks(whereData = {}) {
        return this.selectTasks(whereData, SUPPLIER_TASK);
    }

    /**
     * Return a list of tasks given a where clause and taks type.
     * NOTE: the where clause only supports key '=' value for now.
     *
     * @param {Map<string, string>} [whereData={}] where clause, only equal is supported for now
     * The key is the column name and the value is the value that the column be equal to.
     * @param {string} taskType type of task
     * @param {string} service service
     * @return {object}
     * @private
     */
    async selectTasks(whereData, taskType, service = this.dbCommonHelper.SERVICE_REPLICATION) {
        const where = [];
        for (const [key, value] of Object.entries(whereData)) {
            where.push({ref: [key]}, '=', {val: value});
        }
        const query = {
            SELECT: {
                from: {ref: [`${service}.${taskType}`]},
                where: where
            }
        };

        return this.dbCommonHelper._select(this.service, query);
    }

    /**
     * Query to delete a task and their associated data (jobRuns, packets, company task parameters) from a given system
     * @param {string} taskId id of the task
     * @param {string} service service
     * @returns {Promise<object[]>} CQL queries
     */
    async getQueriesDeleteSupplierTask(taskId, service = this.dbCommonHelper.SERVICE_REPLICATION) {
        const queries = [];

        // Packets
        const subQuery = {
            SELECT: {
                columns: [{ref: ['jobRunId']}],
                from: {ref: [`${service}.${SUPPLIER_JOB_RUN}`]},
                where: [{ref: ['taskId']}, '=', {val: taskId}]
            }
        };
        queries.push({
            DELETE: {
                from: `${service}.${SUPPLIER_PACKET}`,
                where: [{ref: ['jobRunId']}, 'IN', subQuery]
            }
        });


        // JobRun
        queries.push({
            DELETE: {
                from: `${service}.${SUPPLIER_JOB_RUN}`,
                where: [{ref: ['taskId']}, '=', {val: taskId}]
            }
        });

        // Task
        queries.push({
            DELETE: {
                from: `${service}.${SUPPLIER_TASK}`,
                where: [{ref: ['taskid']}, '=', {val: taskId}]
            }
        });
        return queries;
    }

    /**
     * Queries to delete all tasks and their associated data (jobRuns, packets, company task parameters) from a given system
     * @param {object} connectedSystem connected system
     * @param {string} service service
     * @returns {Promise<object[]>} CQL queries
     */
    async getQueriesDeleteTasksOfConnectedSystem(connectedSystem, service = this.dbCommonHelper.SERVICE_REPLICATION) {
        const queries = [];
        for (const taskTable of [SUPPLIER_TASK, COMPANY_TASK]) {

            let tasks;
            if (taskTable === SUPPLIER_TASK) {
                tasks = await this.selectTasks({connectedSystemCode: connectedSystem.code}, taskTable, service);
            } else {

                tasks = await this.selectTasks({connectedSystemId: connectedSystem.id}, taskTable, service);
            }
            if (tasks.length) {
               
            
                const taskIds = tasks.map(task => task.taskId);
                const jobRunTable = taskTable === SUPPLIER_TASK ? SUPPLIER_JOB_RUN : COMPANY_JOB_RUN;
                const packetTable = taskTable === SUPPLIER_TASK ? SUPPLIER_PACKET : COMPANY_PACKET;


                // Packets
                const subQuery = {
                    SELECT: {
                        columns: [{ref: ['jobRunId']}],
                        from: {ref: [`${service}.${jobRunTable}`]},
                        where: [{ref: ['taskId']}, 'IN', {val: taskIds}]
                    }
                };
                queries.push({
                    DELETE: {
                        from: `${service}.${packetTable}`,
                        where: [{ref: ['jobRunId']}, 'IN', subQuery]
                    }
                });

                // JobRuns
                queries.push({
                    DELETE: {
                        from: `${service}.${jobRunTable}`,
                        where: [{ref: ['taskId']}, 'IN', {val: taskIds}]
                    }
                });

                // Tasks
                if (taskTable === SUPPLIER_TASK) {
                    queries.push({
                        DELETE: {
                            from: `${service}.${taskTable}`,
                            where: [{ref: ['connectedSystemCode']}, '=', {val: connectedSystem.code}]
                        }
                    });
                } else {
                    queries.push({
                        DELETE: {
                            from: `${service}.${taskTable}`,
                            where: [{ref: ['connectedSystemId']}, '=', {val: connectedSystem.id}]
                        }
                    });
                }
            }
        }
        return queries;
    }
}

module.exports = TaskDatabaseHelper;
