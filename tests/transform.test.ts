import { Connector, Transformation } from '../src/types';
import Transform from '../src/utils/transform';

describe('Transform Function', () => {
    const sampleData = [
        {
            firstname: 'Jorge',
            lastname: 'Lukas',
            email: 'jorge@supercoolsoftware.com',
            id: '260',
            nickname: '  Lucky  '
        },
        {
            firstname: 'Jane',
            lastname: 'Doe',
            email: 'jane.doe@example.com',
            id: '261',
            nickname: 'JD '
        }
    ];

    const createConnector = (transforms: any[]): Connector => ({
        id: "test-connector",
        adapter_id: "test",
        endpoint_id: "test",
        credential_id: "test",
        fields: ['firstname', 'lastname', 'email', 'id', 'nickname'],
        transform: transforms as Transformation[],
    });

    describe('concat', () => {
        it('concatenates properties into a new field', async () => {
            const connector = createConnector([
                { type: 'concat', options: { properties: ['firstname', 'lastname'], glue: ' ', to: 'full_name' } }
            ]);
            const result = await Transform(connector, sampleData);
            expect(result[0].full_name).toBe('Jorge Lukas');
            expect(result[1].full_name).toBe('Jane Doe');
        });

        it('handles missing properties gracefully', async () => {
            const connector = createConnector([
                { type: 'concat', options: { properties: ['firstname', 'missing'], glue: ' ', to: 'full_name' } }
            ]);
            const result = await Transform(connector, sampleData);
            expect(result[0].full_name).toBe('Jorge');
            expect(result[1].full_name).toBe('Jane');
        });
    });

    describe('renameKey', () => {
        it('renames a key to a new field', async () => {
            const connector = createConnector([
                { type: 'renameKey', options: { from: 'email', to: 'contact_email' } }
            ]);
            const result = await Transform(connector, sampleData);
            expect(result[0].contact_email).toBe('jorge@supercoolsoftware.com');
            expect(result[1].contact_email).toBe('jane.doe@example.com');
        });

        it('handles nested paths', async () => {
            const nestedData = [{ user: { name: 'Jorge' } }];
            const connector = createConnector([
                { type: 'renameKey', options: { from: 'user.name', to: 'username' } }
            ]);
            const result = await Transform(connector, nestedData);
            expect(result[0].username).toBe('Jorge');
        });
    });

    describe('uppercase', () => {
        it('converts a field to uppercase', async () => {
            const connector = createConnector([
                { type: 'uppercase', options: { field: 'firstname', to: 'firstname_upper' } }
            ]);
            const result = await Transform(connector, sampleData);
            expect(result[0].firstname_upper).toBe('JORGE');
            expect(result[1].firstname_upper).toBe('JANE');
        });

        it('overwrites field if no "to" specified', async () => {
            const connector = createConnector([
                { type: 'uppercase', options: { field: 'firstname' } }
            ]);
            const result = await Transform(connector, sampleData);
            expect(result[0].firstname).toBe('JORGE');
            expect(result[1].firstname).toBe('JANE');
        });
    });

    describe('lowercase', () => {
        it('converts a field to lowercase', async () => {
            const connector = createConnector([
                { type: 'lowercase', options: { field: 'firstname', to: 'firstname_lower' } }
            ]);
            const result = await Transform(connector, sampleData);
            expect(result[0].firstname_lower).toBe('jorge');
            expect(result[1].firstname_lower).toBe('jane');
        });
    });

    describe('trim', () => {
        it('trims whitespace from a field', async () => {
            const connector = createConnector([
                { type: 'trim', options: { field: 'nickname', to: 'nickname_trimmed' } }
            ]);
            const result = await Transform(connector, sampleData);
            expect(result[0].nickname_trimmed).toBe('Lucky');
            expect(result[1].nickname_trimmed).toBe('JD');
        });
    });

    describe('split', () => {
        it('splits a field into an array', async () => {
            const connector = createConnector([
                { type: 'split', options: { field: 'email', delimiter: '@', to: 'email_parts' } }
            ]);
            const result = await Transform(connector, sampleData);
            expect(result[0].email_parts).toEqual(['jorge', 'supercoolsoftware.com']);
            expect(result[1].email_parts).toEqual(['jane.doe', 'example.com']);
        });
    });

    describe('replace', () => {
        it('replaces text in a field', async () => {
            const connector = createConnector([
                { type: 'replace', options: { field: 'email', search: '.com', replace: '.org', to: 'email_mod' } }
            ]);
            const result = await Transform(connector, sampleData);
            expect(result[0].email_mod).toBe('jorge@supercoolsoftware.org');
            expect(result[1].email_mod).toBe('jane.doe@example.org');
        });
    });

    describe('addPrefix', () => {
        it('adds a prefix to a field', async () => {
            const connector = createConnector([
                { type: 'addPrefix', options: { field: 'firstname', prefix: 'Mr. ', to: 'formal_name' } }
            ]);
            const result = await Transform(connector, sampleData);
            expect(result[0].formal_name).toBe('Mr. Jorge');
            expect(result[1].formal_name).toBe('Mr. Jane');
        });
    });

    describe('addSuffix', () => {
        it('adds a suffix to a field', async () => {
            const connector = createConnector([
                { type: 'addSuffix', options: { field: 'lastname', suffix: ' Jr.', to: 'full_lastname' } }
            ]);
            const result = await Transform(connector, sampleData);
            expect(result[0].full_lastname).toBe('Lukas Jr.');
            expect(result[1].full_lastname).toBe('Doe Jr.');
        });
    });

    describe('toNumber', () => {
        it('converts a field to a number', async () => {
            const connector = createConnector([
                { type: 'toNumber', options: { field: 'id', to: 'numeric_id' } }
            ]);
            const result = await Transform(connector, sampleData);
            expect(result[0].numeric_id).toBe(260);
            expect(result[1].numeric_id).toBe(261);
        });

        it('defaults to 0 for invalid numbers', async () => {
            const connector = createConnector([
                { type: 'toNumber', options: { field: 'firstname', to: 'numeric_name' } }
            ]);
            const result = await Transform(connector, sampleData);
            expect(result[0].numeric_name).toBe(0);
        });
    });

    describe('extract', () => {
        it('extracts using regex pattern', async () => {
            const connector = createConnector([
                { type: 'extract', options: { field: 'email', pattern: '^(.+)@', to: 'username' } }
            ]);
            const result = await Transform(connector, sampleData);
            expect(result[0].username).toBe('jorge');
            expect(result[1].username).toBe('jane.doe');
        });

        it('extracts using start/end indices', async () => {
            const connector = createConnector([
                { type: 'extract', options: { field: 'email', start: 0, end: 5, to: 'email_start' } }
            ]);
            const result = await Transform(connector, sampleData);
            expect(result[0].email_start).toBe('jorge');
            expect(result[1].email_start).toBe('jane.');
        });
    });

    describe('mergeObjects', () => {
        it('merges fields into a new object', async () => {
            const connector = createConnector([
                { type: 'mergeObjects', options: { fields: ['firstname', 'lastname'], to: 'name' } }
            ]);
            const result = await Transform(connector, sampleData);
            expect(result[0].name).toEqual({ firstname: 'Jorge', lastname: 'Lukas' });
            expect(result[1].name).toEqual({ firstname: 'Jane', lastname: 'Doe' });
        });
    });

    it('handles empty transform array', async () => {
        const connector = createConnector([]);
        const result = await Transform(connector, sampleData);
        expect(result).toEqual(sampleData);
    });

    it('handles no transform property', async () => {
        const connector: Connector = { id: "test", adapter_id: "test", endpoint_id: "test", credential_id: "test", fields: ['firstname'] };
        const result = await Transform(connector, sampleData);
        expect(result).toEqual(sampleData);
    });
});