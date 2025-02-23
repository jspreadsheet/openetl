import {
    Connector,
    ConcatOptions,
    RenameKeyOptions,
    FieldTransformationOptions,
    SplitOptions,
    ReplaceOptions,
    PrefixOptions,
    SuffixOptions,
    ExtractOptions,
    MergeObjectsOptions,
} from '../types';

export default async function Transform(connector: Connector, data: any[]): Promise<any[]> {
    let transformedData = [...data];
    for (const transformation of connector.transform || []) {
        switch (transformation.type) {
            case 'concat': {
                // Narrow options to ConcatOptions
                const options = transformation.options as ConcatOptions;
                const { properties, glue = ' ', to } = options;
                if (properties && to) {
                    transformedData = transformedData.map(item => {
                        const concatenated = properties.map((prop: string) => item[prop]).filter(Boolean).join(glue);
                        return { ...item, [to]: concatenated };
                    });
                }
                break;
            }

            case 'renameKey': {
                const options = transformation.options as RenameKeyOptions;
                const { from, to } = options;
                if (from && to) {
                    transformedData = transformedData.map(item => {
                        const value = from.split('.').reduce((obj, key) => obj?.[key], item);
                        return { ...item, [to]: value };
                    });
                }
                break;
            }

            case 'uppercase': {
                const options = transformation.options as FieldTransformationOptions;
                const { field, to } = options;
                if (field) {
                    transformedData = transformedData.map(item => {
                        const value = item[field]?.toString().toUpperCase() || '';
                        return { ...item, [to || field]: value };
                    });
                }
                break;
            }

            case 'lowercase': {
                const options = transformation.options as FieldTransformationOptions;
                const { field, to } = options;
                if (field) {
                    transformedData = transformedData.map(item => {
                        const value = item[field]?.toString().toLowerCase() || '';
                        return { ...item, [to || field]: value };
                    });
                }
                break;
            }

            case 'trim': {
                const options = transformation.options as FieldTransformationOptions;
                const { field, to } = options;
                if (field) {
                    transformedData = transformedData.map(item => {
                        const value = item[field]?.toString().trim() || '';
                        return { ...item, [to || field]: value };
                    });
                }
                break;
            }

            case 'split': {
                const options = transformation.options as SplitOptions;
                const { field, delimiter, to } = options;
                if (field && delimiter && to) {
                    transformedData = transformedData.map(item => {
                        const value = item[field]?.toString().split(delimiter) || [];
                        return { ...item, [to]: value };
                    });
                }
                break;
            }

            case 'replace': {
                const options = transformation.options as ReplaceOptions;
                const { field, search, replace, to } = options;
                if (field && search && replace !== undefined) {
                    transformedData = transformedData.map(item => {
                        const value = item[field]?.toString().replace(new RegExp(search, 'g'), replace) || '';
                        return { ...item, [to || field]: value };
                    });
                }
                break;
            }

            case 'addPrefix': {
                const options = transformation.options as PrefixOptions;
                const { field, prefix, to } = options;
                if (field && prefix) {
                    transformedData = transformedData.map(item => {
                        const value = `${prefix}${item[field] || ''}`;
                        return { ...item, [to || field]: value };
                    });
                }
                break;
            }

            case 'addSuffix': {
                const options = transformation.options as SuffixOptions;
                const { field, suffix, to } = options;
                if (field && suffix) {
                    transformedData = transformedData.map(item => {
                        const value = `${item[field] || ''}${suffix}`;
                        return { ...item, [to || field]: value };
                    });
                }
                break;
            }

            case 'toNumber': {
                const options = transformation.options as FieldTransformationOptions;
                const { field, to } = options;
                if (field) {
                    transformedData = transformedData.map(item => {
                        const value = parseFloat(item[field]?.toString()) || 0;
                        return { ...item, [to || field]: isNaN(value) ? 0 : value };
                    });
                }
                break;
            }

            case 'extract': {
                const options = transformation.options as ExtractOptions;
                const { field, pattern, start, end, to } = options;
                if (field && to) {
                    transformedData = transformedData.map(item => {
                        const value = item[field]?.toString() || '';
                        if (pattern) {
                            const match = value.match(new RegExp(pattern));
                            return { ...item, [to]: match ? match[1] || match[0] : '' };
                        } else if (start !== undefined && end !== undefined) {
                            return { ...item, [to]: value.slice(start, end) };
                        }
                        return item;
                    });
                }
                break;
            }

            case 'mergeObjects': {
                const options = transformation.options as MergeObjectsOptions;
                const { fields, to } = options;
                if (fields && to) {
                    transformedData = transformedData.map(item => {
                        const merged = fields.reduce((obj: Record<string, any>, field) => {
                            if (item[field] !== undefined) {
                                obj[field] = item[field];
                            }
                            return obj;
                        }, {});
                        return { ...item, [to]: merged };
                    });
                }
                break;
            }

            default:
                console.warn(`Unknown transformation type: ${transformation.type}`);
                break;
        }
    }

    return transformedData;
}