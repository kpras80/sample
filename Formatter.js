class Formatter  {

format(result, entities, entityType, parentEntity) {
        if (!Array.isArray(entities)) {
            entities = [entities];
        }
        const tableName = this.mapping.tableMapping[entityType].cimTableName;
        const validatorfn = this.mapping.tableMapping[entityType].validator;
        const propNames = Object.keys(this.mapping.entities[tableName]);
        entities.forEach((entity) => {
            const row = [];
            const entityRecord = {};
            const start = Date.now();
            propNames.forEach((propName) => {
                // Get the matching value
                let value = this.mapping.entities[tableName][propName](this.options, entity, parentEntity);
                const cdsProperty = this.getProperty(tableName, propName);
                // Convert to the correct type
                if (cdsProperty) {
                    value = this.convertValue(value, cdsProperty.type);
                }
                row.push(value);
                entityRecord[propName] = value;
            });
            result[tableName].statistics.timeTaken += (Date.now() - start);
            const isValid = validatorfn ? validatorfn(entityRecord) : true;
            if (isValid) {
                result[tableName].statistics.processed++;
                result[tableName].rows.push(row);
                // populate dependant tables
                const relationships = this.mapping.tableMapping[entityType].relations;
                if (relationships) {
                    for (const relationshipEntityType in relationships) {
                        let relations = relationships[relationshipEntityType];
                        // process each relation fragment in the result
                        for (const relation of relations) {
                            // recursively process each relationship fragment
                            // currently assumes that only the immediate parent entity is required for processing dependents
                            // TOODO revisit this when dealing with supplier entity
                            this.format(result, entity[relation], relationshipEntityType, entityRecord);
   
                        }
                    }
                }
            } else {
                result[tableName].statistics.skipped++;
            }
        });
    }
    }
    
    module.exports = ConverterV2;
