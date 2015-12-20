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
    var jv = jsvalidate(ob, jobSchema, {throwError:true});
    return jv.valid;
}

function isValid(ob){
    var jv = jsvalidate(ob, jobSchema, {throwError:false});
    return jv.valid;
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

/* Transform an object into a valid Job object.
 *
 * Throws an exception when invalid parameters are specified (validation fails).
 */
function fromObject(ob){
    var job = {};
    var key, val, typ;
    for(key in ob){
        if (ob.hasOwnProperty(key)){
            typ = jobSchema.properties[key].type;
            if(SchemaType.hasOwnProperty(typ)){
                val = SchemaType[typ](ob[key]);
            } else {
                val = String(ob[key]);
            }
            job[key] = val;
        }
    }
    validate(job);
    return job;
}

/* Transform a string array into a Job object.
 *
 * Throws an exception when invalid parameters are specified (validation fails).
 */
function fromStringArray(arr){
    var job = {};
    var i;
    for(i=0; i<arr.length; i=i+2){
        job[arr[i]] = arr[i+1];
    }
    return fromObject(job);
}


module.exports = {
    validate: validate,
    isValid: isValid,
    fromStringArray: fromStringArray,
    toStringArray: toStringArray,
    fromObject: fromObject,
    jobSchema: jobSchema,
    SchemaType: SchemaType
};

// function fromStringArray(arr){
//     var job = {};
//     var key, val, i, typ;
//     for(i=0; i<arr.length; i=i+2){
//         key = arr[i];
//         // convert from string to type
//         typ = jobSchema.properties[key].type;
//         if(SchemaType.hasOwnProperty(typ)){
//             val = SchemaType[typ](arr[i+1]);
//         } else {
//             val = String(arr[i+1]);
//         }
//         job[key] = val;
//     }
//     // validate will throw an error
//     validate(job);
//     return job;
// }

// function fromObject(ob){
//     var arr = [];
//     var key;
//     for(key in ob){
//         if (ob.hasOwnProperty(key)){
//             arr.push(key);
//             arr.push(ob[key]);
//         }
//     }
//     return fromStringArray(arr);
// }
