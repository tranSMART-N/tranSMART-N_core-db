package org.transmartproject.db.querytool

import org.transmartproject.db.i2b2data.PatientDimension

class QtPatientSetCollection {

	Long            setIndex

    static belongsTo = [
            resultInstance: QtQueryResultInstance,
            patient:        PatientDimension,
    ]

	static mapping = {
        table          schema:   'I2B2DEMODATA'
        id          generator: 'sequence', params:[sequence:'qt_sq_qpr_pcid']
        id             column:   "patient_set_coll_id"//, generator: "identity"
        resultInstance column:   'result_instance_id'
        patient        column:   'patient_num'

        sort           setIndex: 'asc'

		version false
	}

	static constraints = {
        resultInstance   nullable: true
		setIndex         nullable: true
		patient          nullable: true
	}
}
