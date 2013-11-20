package org.transmartproject.db.querytool

import org.hibernate.jdbc.Work
import org.transmartproject.core.exceptions.InvalidRequestException
import org.transmartproject.core.exceptions.NoSuchResourceException
import org.transmartproject.core.querytool.QueriesResource
import org.transmartproject.core.querytool.QueryDefinition
import org.transmartproject.core.querytool.QueryResult
import org.transmartproject.core.querytool.QueryStatus

import java.sql.Connection

//import groovy.sql.*

class QueriesResourceService implements QueriesResource {

    def dataSource
    def grailsApplication
    def patientSetQueryBuilderService
    def queryDefinitionXmlService
    def sessionFactory

    @Override
    QueryResult runQuery(QueryDefinition definition, String datbaseType) throws InvalidRequestException {
        if (datbaseType.toLowerCase().equals("netezza")) runNetezzaQuery(definition)
        else runQuery(definition)
    }

    @Override
    QueryResult runNetezzaQuery(QueryDefinition definition) throws InvalidRequestException {

        // 1. Populate qt_query_master
//        QtQueryMaster queryMaster = new QtQueryMaster(
//                name: definition.name,
//                userId: grailsApplication.config.org.transmartproject.i2b2.user_id,
//                groupId: grailsApplication.config.org.transmartproject.i2b2.group_id,
//                createDate: new Date(),
//                generatedSql: null,
//                requestXml: queryDefinitionXmlService.toXml(definition),
//                i2b2RequestXml: null,
//        )

        groovy.sql.Sql nsql = new groovy.sql.Sql(dataSource)

        nsql.execute("SET SERIALIZABLE=0")

        String query
        String name = definition.name
        String userId = grailsApplication.config.org.transmartproject.i2b2.user_id
        String groupId = grailsApplication.config.org.transmartproject.i2b2.group_id
        String requestXml = queryDefinitionXmlService.toXml(definition)

        def queryMasterId
        def m = nsql.firstRow("select next value for QT_SQ_QM_QMID")
        m.each { k, v ->
            queryMasterId = v
        }

        nsql.execute("SET SERIALIZABLE=0")
        query = "insert into qt_query_master(QUERY_MASTER_ID, NAME, USER_ID, GROUP_ID, CREATE_DATE, REQUEST_XML) values(?, ?, ?, ?, now(), ?) "
        nsql.execute(query, [queryMasterId, name, userId, groupId, requestXml])

        // 2. Populate qt_query_instance
//        QtQueryInstance queryInstance = new QtQueryInstance(
//                userId: grailsApplication.config.org.transmartproject.i2b2.user_id,
//                groupId: grailsApplication.config.org.transmartproject.i2b2.group_id,
//                startDate: new Date(),
//                statusTypeId: QueryStatus.PROCESSING.id,
//                queryMaster: queryMaster,
//        )
//        queryMaster.addToQueryInstances(queryInstance)

        def queryInstanceId
        def m2 = nsql.firstRow("select next value for QT_SQ_QI_QIID")
        m2.each { k, v ->
            queryInstanceId = v
        }

        nsql.execute("SET SERIALIZABLE=0")
        query = "insert into qt_query_instance(QUERY_INSTANCE_ID, QUERY_MASTER_ID, USER_ID, GROUP_ID, STATUS_TYPE_ID, START_DATE) values(?, ?, ?, ?, ?, now())"
        nsql.execute(query, [queryInstanceId, queryMasterId, userId, groupId, QueryStatus.PROCESSING.id])

        QtQueryInstance queryInstance = (new QtQueryInstance()).get(queryInstanceId)

        // 3. Populate qt_query_result_instance
//        QtQueryResultInstance resultInstance = new QtQueryResultInstance(
//                statusTypeId: QueryStatus.PROCESSING.id,
//                startDate: new Date() //,
////                queryInstance: queryInstance
//        )
//        queryInstance.addToQueryResults(resultInstance)
//
//        // 4. Save the three objects
//        if (!queryMaster.validate()) {
//            throw new InvalidRequestException('Could not create a valid ' +
//                    'QtQueryMaster: ' + queryMaster.errors)
//        }
//        if (queryMaster.save() == null) {
//            throw new RuntimeException('Failure saving QtQueryMaster')
//        }

        def queryResultInstanceId
        def m3 = nsql.firstRow("select next value for QT_SQ_QRI_QRIID")
        m3.each { k, v ->
            queryResultInstanceId = v
        }

        nsql.execute("SET SERIALIZABLE=0")
        query = "insert into QT_QUERY_RESULT_INSTANCE(RESULT_INSTANCE_ID, QUERY_INSTANCE_ID, RESULT_TYPE_ID, STATUS_TYPE_ID, START_DATE) values(?, ?, 1, ?, now())"
        nsql.execute(query, [queryResultInstanceId, queryInstanceId, QueryStatus.PROCESSING.id])

        QtQueryResultInstance resultInstance = (new QtQueryResultInstance()).get(queryResultInstanceId)

        // 5. Flush session so objects are inserted & raw SQL can access them
        sessionFactory.currentSession.flush()

        // 6. Build the patient set
        def setSize
        def sql
        try {
            sql = patientSetQueryBuilderService.buildPatientSetQuery(
                    resultInstance, definition)

            sessionFactory.currentSession.doWork({ Connection conn ->
                def statement
                // HX 2013-10-31
//                statement = conn.prepareStatement('SAVEPOINT doWork')
//                statement = conn.prepareStatement()
//                statement.execute()

                statement = conn.prepareStatement("SET SERIALIZABLE=0")
                statement.execute()

                statement = conn.prepareStatement(sql)
                setSize = statement.executeUpdate()

                log.debug "Inserted $setSize rows into qt_patient_set_collection"
            } as Work)
        } catch (InvalidRequestException e) {
            log.error 'Invalid request; rollong back transaction', e
            throw e /* unchecked; rolls back transaction */
        } catch (Exception e) {
            // 6e. Handle error when building/running patient set query
            log.error 'Error running (or build) querytool SQL query, ' +
                    "failing query was '$sql'", e

            // Rollback to save point
            sessionFactory.currentSession.createSQLQuery(
//                    'ROLLBACK TO SAVEPOINT doWork').executeUpdate()
                    'ROLLBACK').executeUpdate()

            StringWriter sw = new StringWriter()
            e.printStackTrace(new PrintWriter(sw, true))

            resultInstance.setSize = resultInstance.realSetSize = -1
            resultInstance.endDate = new Date()
            resultInstance.statusTypeId = QueryStatus.ERROR.id
            resultInstance.errorMessage = sw.toString()

            queryInstance.endDate = new Date()
            queryInstance.statusTypeId = QueryStatus.ERROR.id
            queryInstance.message = sw.toString()

            nsql.execute("SET SERIALIZABLE=0")
            if (!resultInstance.save()) {
                log.error("After exception from " +
                        "patientSetQueryBuilderService::buildService, " +
                        "failed saving updated resultInstance and " +
                        "queryInstance")
            }
            return resultInstance
        }

        // 7. Update result instance and query instance
        resultInstance.setSize = resultInstance.realSetSize = setSize
        resultInstance.description = "Patient set for \"${definition.name}\""
        resultInstance.endDate = new Date()
        resultInstance.statusTypeId = QueryStatus.FINISHED.id

        queryInstance.endDate = new Date()
        queryInstance.statusTypeId = QueryStatus.COMPLETED.id

        nsql.execute("SET SERIALIZABLE=0")
        def newResultInstance = resultInstance.save()
        if (!newResultInstance) {
            throw new RuntimeException('Failure saving resultInstance after ' +
                    'successfully building patient set. Errors: ' +
                    resultInstance.errors)
        }

        // 8. Return result instance
        resultInstance
    }

    @Override
    QueryResult runQuery(QueryDefinition definition) throws InvalidRequestException {

        // 1. Populate qt_query_master
        QtQueryMaster queryMaster = new QtQueryMaster(
                name: definition.name,
                userId: grailsApplication.config.org.transmartproject.i2b2.user_id,
                groupId: grailsApplication.config.org.transmartproject.i2b2.group_id,
                createDate: new Date(),
                generatedSql: null,
                requestXml: queryDefinitionXmlService.toXml(definition),
                i2b2RequestXml: null,
        )

        // 2. Populate qt_query_instance
        QtQueryInstance queryInstance = new QtQueryInstance(
                userId: grailsApplication.config.org.transmartproject.i2b2.user_id,
                groupId: grailsApplication.config.org.transmartproject.i2b2.group_id,
                startDate: new Date(),
                statusTypeId: QueryStatus.PROCESSING.id,
                queryMaster: queryMaster,
        )
        queryMaster.addToQueryInstances(queryInstance)

        // 3. Populate qt_query_result_instance
        QtQueryResultInstance resultInstance = new QtQueryResultInstance(
                statusTypeId: QueryStatus.PROCESSING.id,
                startDate: new Date(),
                queryInstance: queryInstance
        )
        queryInstance.addToQueryResults(resultInstance)

        // 4. Save the three objects
        if (!queryMaster.validate()) {
            throw new InvalidRequestException('Could not create a valid ' +
                    'QtQueryMaster: ' + queryMaster.errors)
        }
        if (queryMaster.save() == null) {
            throw new RuntimeException('Failure saving QtQueryMaster')
        }

        // 5. Flush session so objects are inserted & raw SQL can access them
        sessionFactory.currentSession.flush()

        // 6. Build the patient set
        def setSize
        def sql
        try {
            sql = patientSetQueryBuilderService.buildPatientSetQuery(
                    resultInstance, definition)

            sessionFactory.currentSession.doWork({ Connection conn ->
                def statement
                // HX 2013-10-31
//                statement = conn.prepareStatement('SAVEPOINT doWork')
//                statement = conn.prepareStatement()
//                statement.execute()

                statement = conn.prepareStatement(sql)
                setSize = statement.executeUpdate()
//                setSize = statement.execute()

                log.debug "Inserted $setSize rows into qt_patient_set_collection"
            } as Work)
        } catch (InvalidRequestException e) {
            log.error 'Invalid request; rollong back transaction', e
            throw e /* unchecked; rolls back transaction */
        } catch (Exception e) {
            // 6e. Handle error when building/running patient set query
            log.error 'Error running (or build) querytool SQL query, ' +
                    "failing query was '$sql'", e

            // Rollback to save point
            sessionFactory.currentSession.createSQLQuery(
//                    'ROLLBACK TO SAVEPOINT doWork').executeUpdate()
                    'ROLLBACK').executeUpdate()

            StringWriter sw = new StringWriter()
            e.printStackTrace(new PrintWriter(sw, true))

            resultInstance.setSize = resultInstance.realSetSize = -1
            resultInstance.endDate = new Date()
            resultInstance.statusTypeId = QueryStatus.ERROR.id
            resultInstance.errorMessage = sw.toString()

            queryInstance.endDate = new Date()
            queryInstance.statusTypeId = QueryStatus.ERROR.id
            queryInstance.message = sw.toString()

            if (!resultInstance.save()) {
                log.error("After exception from " +
                        "patientSetQueryBuilderService::buildService, " +
                        "failed saving updated resultInstance and " +
                        "queryInstance")
            }
            return resultInstance
        }

        // 7. Update result instance and query instance
        resultInstance.setSize = resultInstance.realSetSize = setSize
        resultInstance.description = "Patient set for \"${definition.name}\""
        resultInstance.endDate = new Date()
        resultInstance.statusTypeId = QueryStatus.FINISHED.id

        queryInstance.endDate = new Date()
        queryInstance.statusTypeId = QueryStatus.COMPLETED.id

        def newResultInstance = resultInstance.save()
        if (!newResultInstance) {
            throw new RuntimeException('Failure saving resultInstance after ' +
                    'successfully building patient set. Errors: ' +
                    resultInstance.errors)
        }

        // 8. Return result instance
        resultInstance
    }

    @Override
    QueryResult getQueryResultFromId(Long id) throws NoSuchResourceException {
        QtQueryResultInstance.load(id)
    }

    @Override
    QueryDefinition getQueryDefinitionForResult(QueryResult result)
    throws NoSuchResourceException {
        List answer = QtQueryResultInstance.executeQuery(
                '''SELECT R.queryInstance.queryMaster.requestXml FROM
                        QtQueryResultInstance R WHERE R = ?''',
               [result]
        )
        if (!answer) {
            throw new NoSuchResourceException('Could not find definition for ' +
                    'query result with id=' + result.id)
        }

        queryDefinitionXmlService.fromXml(new StringReader(answer[0]))
    }
}
