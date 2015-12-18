var jsvalidate = require('jsonschema').validate;

var jobSchema = {
    id: "/Job",
    type: "object",
    properties: {
        'id': { type: 'string' },
        'priority': { type: 'number' },
        'payload': { type: 'string' },
        'created': { type: 'number', description: 'Date of job creation (addition to the queue system.) Unix time; seconds.' },
        'failures': { type: 'number' },
        'failed': { type: 'number', description: 'Date of last failure. Unix time; seconds since epoch.' },
        'consumed': { type: 'number', description: 'Date last consumed. Unix time; seconds since epoch.' },
        'owner': { type: 'string', description: 'String identifier for the worker that consumed the job.' },
        'contenttype': { type: 'string', description: 'Content-Type of the payload.' },
        'encoding': { type: 'string', description: 'Encoding of the payload' },
        'state': { type: {'enum': ['enqueued' ,'consumed', 'failed', 'scheduled']},
                   description: 'The current state of the job in the job system.'
        }
    },
    required: ['id', 'priority', 'payload'],
    additionalProperties: false
};

/* Validate an object against the Job schema.
 *
 * Throws an exception when validation fails.
 */
function validate(ob){
    return jsvalidate(ob, jobSchema, {throwError:true}).valid;
}

/* Returns the job object as an array of strings.
 *
 * Throws an exception if the job does not validate.
 */
function toStringArray(job){
    validate(job);
    var arr = [];
    var key;
    for (key in job){
        if(job.hasOwnProperty(key)
           && jobSchema.properties.hasOwnProperty(key)
           && (job[key] !== undefined)){
                arr.push(key);
                arr.push(job[key].toString());
        }
    }
    return arr;
}

var SchemaType = {
    'number': Number,
    'integer': Number,
    'string': String
};

/* Transform a string array into a Job object.
 *
 * Throws an exception when invalid parameters are specified (validation fails).
 */
function fromStringArray(arr){
    var job = {};
    var key, val, i;
    for(i=0; i<arr.length; i=i+2){
        key = arr[i];
        // convert from string to type
        val = SchemaType[jobSchema.properties[key].type](arr[i+1]);
        job[key] = val;
    }
    // validate will throw an error
    validate(job);
    return job;
}

module.exports = {
    validate: validate,
    fromStringArray: fromStringArray,
    toStringArray: toStringArray,
    jobSchema: jobSchema,
    // Job: Job,
    SchemaType: SchemaType
};

/* Job object constructor.
 * Required parameters: 'id', 'priority', and 'payload'.
 */
// function Job(id, priority, payload, contenttype, encoding) {
//     if(!id || !priority || !payload) {
//         throw new Error("Invalid Job parameters.");
//     }
//     this.id = id;
//     this.priority = priority;
//     this.payload = payload;
//     this.contenttype = contenttype;
//     this.encoding = encoding;
//     this.created = new Date().getTime();
// }

// Job.prototype.toStringArray = function toStringArray(){
//     var arr = [];
//     var key;
//     for (key in this){
//         if(this.hasOwnProperty(key)
//            && jobSchema.properties.hasOwnProperty(key)
//            && (this[key] !== undefined)){
//                 arr.push(key);
//                 arr.push(this[key].toString());
//         }
//     }
//     return arr;
// };

