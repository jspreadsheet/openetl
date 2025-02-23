# HubSpot Adapter

- Description: A static catalog of HubSpot CRM and Marketing API endpoints for ETL pipelines.
- ID: hubspot-adapter
- Name: HubSpot CRM Adapter
- Type: http
- Actions: download, upload, sync
- Credential Type: api_key
- Base URL: https://api.hubapi.com
- Metadata:
  - Provider: hubspot
  - Description: Adapter for HubSpot CRM and Marketing APIs
  - Version: v3

## Endpoints

### CRM Objects

- ID: contacts
  - Path: /crm/v3/objects/contacts
  - Method: GET
  - Description: Retrieve all contacts from HubSpot
  - Supported Actions: download, sync

- ID: create-contact
  - Path: /crm/v3/objects/contacts
  - Method: POST
  - Description: Create a new contact in HubSpot
  - Supported Actions: upload

- ID: companies
  - Path: /crm/v3/objects/companies
  - Method: GET
  - Description: Retrieve all companies from HubSpot
  - Supported Actions: download, sync

- ID: create-company
  - Path: /crm/v3/objects/companies
  - Method: POST
  - Description: Create a new company in HubSpot
  - Supported Actions: upload

- ID: deals
  - Path: /crm/v3/objects/deals
  - Method: GET
  - Description: Retrieve all deals from HubSpot
  - Supported Actions: download, sync

- ID: create-deal
  - Path: /crm/v3/objects/deals
  - Method: POST
  - Description: Create a new deal in HubSpot
  - Supported Actions: upload

- ID: tickets
  - Path: /crm/v3/objects/tickets
  - Method: GET
  - Description: Retrieve all support tickets from HubSpot
  - Supported Actions: download, sync

- ID: create-ticket
  - Path: /crm/v3/objects/tickets
  - Method: POST
  - Description: Create a new support ticket in HubSpot
  - Supported Actions: upload

- ID: products
  - Path: /crm/v3/objects/products
  - Method: GET
  - Description: Retrieve all products from HubSpot
  - Supported Actions: download, sync

- ID: create-product
  - Path: /crm/v3/objects/products
  - Method: POST
  - Description: Create a new product in HubSpot
  - Supported Actions: upload

### Marketing Endpoints

- ID: marketing-emails
  - Path: /marketing/v3/emails
  - Method: GET
  - Description: Retrieve all marketing emails from HubSpot
  - Supported Actions: download, sync

- ID: create-marketing-email
  - Path: /marketing/v3/emails
  - Method: POST
  - Description: Create a new marketing email in HubSpot
  - Supported Actions: upload

- ID: forms
  - Path: /forms/v2/forms
  - Method: GET
  - Description: Retrieve all forms from HubSpot
  - Supported Actions: download, sync

- ID: create-form
  - Path: /forms/v2/forms
  - Method: POST
  - Description: Create a new form in HubSpot
  - Supported Actions: upload

### Analytics Endpoints

- ID: analytics-events
  - Path: /events/v3/events
  - Method: GET
  - Description: Retrieve analytics events from HubSpot
  - Supported Actions: download, sync

### Engagements (Activities)

- ID: engagements
  - Path: /engagements/v1/engagements
  - Method: GET
  - Description: Retrieve all engagements (notes, emails, calls, etc.)
  - Supported Actions: download, sync

- ID: create-engagement
  - Path: /engagements/v1/engagements
  - Method: POST
  - Description: Create a new engagement (e.g., note, email, call)
  - Supported Actions: upload

### Pipelines

- ID: pipelines
  - Path: /crm/v3/pipelines/deals
  - Method: GET
  - Description: Retrieve all deal pipelines from HubSpot
  - Supported Actions: download, sync

- ID: ticket-pipelines
  - Path: /crm/v3/pipelines/tickets
  - Method: GET
  - Description: Retrieve all ticket pipelines from HubSpot
  - Supported Actions: download, sync

### Owners

- ID: owners
  - Path: /crm/v3/owners
  - Method: GET
  - Description: Retrieve all owners (users) in HubSpot
  - Supported Actions: download, sync