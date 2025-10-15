-- ------------------------------
-- OPTION
-- ------------------------------

OPTION IMPORT;

-- ------------------------------
-- NAMESPACE
-- ------------------------------

-- Create a NAMESPACE if it does not already exist
DEFINE NAMESPACE IF NOT EXISTS test;
USE NAMESPACE test_surreal;

-- ------------------------------
-- DATABASE
-- ------------------------------

-- Create a DATABASE if it does not already exist
DEFINE DATABASE IF NOT EXISTS test;
USE DATABASE test_surreal;

-- ------------------------------
-- TABLE: cards
-- ------------------------------

DEFINE TABLE cards TYPE NORMAL SCHEMAFULL PERMISSIONS NONE;

DEFINE FIELD description ON cards TYPE string PERMISSIONS FULL;
DEFINE FIELD image ON cards TYPE string PERMISSIONS FULL;
DEFINE FIELD name ON cards TYPE string PERMISSIONS FULL;
DEFINE FIELD reference ON cards TYPE record<modules | setups> PERMISSIONS FULL;
DEFINE FIELD short_description ON cards TYPE string PERMISSIONS FULL;
DEFINE FIELD tags ON cards TYPE array<record<tags>> PERMISSIONS FULL;
DEFINE FIELD tags[*] ON cards TYPE record<tags> PERMISSIONS FULL;
DEFINE FIELD created_at ON cards TYPE datetime DEFAULT time::now() READONLY PERMISSIONS FULL;
DEFINE FIELD updated_at ON cards TYPE datetime VALUE time::now() PERMISSIONS FULL;

-- ------------------------------
-- TABLE: costs
-- ------------------------------

DEFINE TABLE costs TYPE NORMAL SCHEMAFULL PERMISSIONS NONE;

DEFINE FIELD cost ON costs TYPE number PERMISSIONS FULL;
DEFINE FIELD mission_id ON costs TYPE record<missions> PERMISSIONS FULL;
DEFINE FIELD name ON costs TYPE string PERMISSIONS FULL;
DEFINE FIELD cost_type ON costs TYPE string PERMISSIONS FULL;
DEFINE FIELD unit ON costs TYPE string PERMISSIONS FULL;
DEFINE FIELD rate ON costs TYPE number PERMISSIONS FULL;
DEFINE FIELD quantity ON costs TYPE number PERMISSIONS FULL;
DEFINE FIELD setup_version_id ON costs TYPE record<setup_versions> PERMISSIONS FULL;

DEFINE INDEX costsMissionIdNameIndex ON costs FIELDS mission_id, name;

DEFINE INDEX costsMissionIdNameIndex ON costs FIELDS mission_id, name;
-- ------------------------------
-- TABLE: filesystem
-- ------------------------------

DEFINE TABLE filesystem TYPE NORMAL SCHEMAFULL PERMISSIONS NONE;

DEFINE FIELD context ON filesystem TYPE record<missions> | record<setups> PERMISSIONS FULL;
DEFINE FIELD name ON filesystem TYPE string PERMISSIONS FULL;
DEFINE FIELD file_type ON filesystem TYPE string PERMISSIONS FULL;
DEFINE FIELD content_type ON filesystem TYPE string PERMISSIONS FULL;
DEFINE FIELD size_bytes ON filesystem TYPE int PERMISSIONS FULL;
DEFINE FIELD checksum ON filesystem TYPE string PERMISSIONS FULL;
DEFINE FIELD metadata ON filesystem FLEXIBLE TYPE object PERMISSIONS FULL;
DEFINE FIELD storage_uri ON filesystem TYPE string PERMISSIONS FULL;
DEFINE FIELD file_url ON filesystem TYPE string PERMISSIONS FULL;
DEFINE FIELD status ON filesystem TYPE string PERMISSIONS FULL;
DEFINE FIELD created_at ON filesystem TYPE datetime DEFAULT time::now() READONLY PERMISSIONS FULL;
DEFINE FIELD updated_at ON filesystem TYPE datetime VALUE time::now() PERMISSIONS FULL;

-- ------------------------------
-- TABLE: missions
-- ------------------------------

DEFINE TABLE missions TYPE NORMAL SCHEMAFULL PERMISSIONS NONE;

DEFINE FIELD name ON missions TYPE string PERMISSIONS FULL;
DEFINE FIELD organization_id ON missions TYPE record<organizations> PERMISSIONS FULL;
DEFINE FIELD setup_id ON missions TYPE record<setups> PERMISSIONS FULL;
DEFINE FIELD user_id ON missions TYPE record<users> PERMISSIONS FULL;
DEFINE FIELD created_at ON missions TYPE datetime DEFAULT time::now() READONLY PERMISSIONS FULL;
DEFINE FIELD updated_at ON missions TYPE datetime VALUE time::now() PERMISSIONS FULL;

-- ------------------------------
-- TABLE: modules
-- ------------------------------

DEFINE TABLE modules TYPE NORMAL SCHEMALESS PERMISSIONS NONE;

DEFINE FIELD address ON modules TYPE string PERMISSIONS FULL;
DEFINE FIELD cost_schema ON modules TYPE object PERMISSIONS FULL;
DEFINE FIELD input_schema ON modules TYPE object PERMISSIONS FULL;
DEFINE FIELD module_type ON modules TYPE string PERMISSIONS FULL;
DEFINE FIELD name ON modules TYPE string PERMISSIONS FULL;
DEFINE FIELD organization_id ON modules TYPE record<organizations> PERMISSIONS FULL;
DEFINE FIELD output_schema ON modules TYPE object PERMISSIONS FULL;
DEFINE FIELD owner ON modules TYPE record<users> PERMISSIONS FULL;
DEFINE FIELD port ON modules TYPE int PERMISSIONS FULL;
DEFINE FIELD secret_schema ON modules TYPE object PERMISSIONS FULL;
DEFINE FIELD setup_schema ON modules TYPE object PERMISSIONS FULL;
DEFINE FIELD version ON modules TYPE int PERMISSIONS FULL;
DEFINE FIELD visibility ON modules TYPE 'PUBLIC' | 'PRIVATE' | 'INTERNAL' PERMISSIONS FULL;
DEFINE FIELD created_at ON modules TYPE datetime DEFAULT time::now() READONLY PERMISSIONS FULL;
DEFINE FIELD updated_at ON modules TYPE datetime VALUE time::now() PERMISSIONS FULL;

-- ------------------------------
-- TABLE: setup_versions
-- ------------------------------

DEFINE TABLE setup_versions TYPE NORMAL SCHEMAFULL PERMISSIONS NONE;

DEFINE FIELD content ON setup_versions FLEXIBLE TYPE object PERMISSIONS FULL;
DEFINE FIELD setup_id ON setup_versions TYPE record<setups> PERMISSIONS FULL;
DEFINE FIELD version ON setup_versions TYPE string PERMISSIONS FULL;
DEFINE FIELD created_at ON setup_versions TYPE datetime DEFAULT time::now() READONLY PERMISSIONS FULL;
DEFINE FIELD updated_at ON setup_versions TYPE datetime DEFAULT time::now() PERMISSIONS FULL;

-- ------------------------------
-- TABLE: setups
-- ------------------------------

DEFINE TABLE setups TYPE NORMAL SCHEMAFULL PERMISSIONS NONE;

DEFINE FIELD current_setup_version_id ON setups TYPE option<record<setup_versions>> PERMISSIONS FULL;
DEFINE FIELD module_id ON setups TYPE record<modules> PERMISSIONS FULL;
DEFINE FIELD name ON setups TYPE string PERMISSIONS FULL;
DEFINE FIELD organization_id ON setups TYPE record<organizations> PERMISSIONS FULL;
DEFINE FIELD owner_id ON setups TYPE record<users> PERMISSIONS FULL;
DEFINE FIELD created_at ON setups TYPE datetime DEFAULT time::now() READONLY PERMISSIONS FULL;
DEFINE FIELD updated_at ON setups TYPE datetime VALUE time::now() PERMISSIONS FULL;

-- ------------------------------
-- TABLE: snapshot
-- ------------------------------

DEFINE TABLE snapshot TYPE NORMAL SCHEMAFULL PERMISSIONS NONE;

DEFINE FIELD data ON snapshot TYPE object PERMISSIONS FULL;
DEFINE FIELD data_repr ON snapshot TYPE object PERMISSIONS FULL;
DEFINE FIELD mission_id ON snapshot TYPE record<missions> PERMISSIONS FULL;
DEFINE FIELD step ON snapshot TYPE string PERMISSIONS FULL;
DEFINE FIELD version ON snapshot TYPE string PERMISSIONS FULL;
DEFINE FIELD created_at ON snapshot TYPE datetime DEFAULT time::now() READONLY PERMISSIONS FULL;
DEFINE FIELD updated_at ON snapshot TYPE datetime VALUE time::now() PERMISSIONS FULL;

-- ------------------------------
-- TABLE: storage
-- ------------------------------

DEFINE TABLE storage TYPE NORMAL SCHEMAFULL PERMISSIONS NONE;

DEFINE FIELD collection     ON storage          TYPE string                                 ASSERT $value != NULL   PERMISSIONS FULL;
DEFINE FIELD record_id      ON storage          TYPE string                                 ASSERT $value != NULL   PERMISSIONS FULL;
DEFINE FIELD mission_id     ON storage          TYPE record<missions>                       ASSERT $value != NULL   PERMISSIONS FULL;
DEFINE FIELD data_type      ON storage          TYPE 'OUTPUT' | 'VIEW' | 'LOGS' | 'OTHER'   ASSERT $value != NULL   PERMISSIONS FULL;
DEFINE FIELD data           ON storage FLEXIBLE TYPE object                                                         PERMISSIONS FULL;
DEFINE FIELD created_at     ON storage          TYPE datetime DEFAULT time::now() READONLY                          PERMISSIONS FULL;
DEFINE FIELD updated_at     ON storage          TYPE datetime VALUE time::now()                                     PERMISSIONS FULL;

DEFINE INDEX storageMissionIdCollectionIdIndex          ON storage FIELDS mission_id, collection;
-- DEFINE INDEX storageMissionIdCollectionIdRecordIdIndex  ON storage FIELDS mission_id, collection, record_id UNIQUE;

-- ------------------------------
-- TABLE: tags
-- ------------------------------

DEFINE TABLE tags TYPE NORMAL SCHEMAFULL PERMISSIONS NONE;

DEFINE FIELD name ON tags TYPE string PERMISSIONS FULL;


-- ------------------------------
-- TABLE: idp
-- ------------------------------

DEFINE TABLE idp TYPE ANY SCHEMALESS PERMISSIONS NONE;

DEFINE FIELD completedAt ON idp TYPE option<datetime> PERMISSIONS FULL;
DEFINE FIELD createdAt ON idp TYPE datetime DEFAULT time::now() READONLY PERMISSIONS FULL;
DEFINE FIELD error ON idp FLEXIBLE TYPE option<object> PERMISSIONS FULL;
DEFINE FIELD expiresAt ON idp TYPE datetime PERMISSIONS FULL;
DEFINE FIELD key ON idp TYPE string PERMISSIONS FULL;
DEFINE FIELD partialResult ON idp FLEXIBLE PERMISSIONS FULL;
DEFINE FIELD result ON idp FLEXIBLE PERMISSIONS FULL;
DEFINE FIELD status ON idp TYPE string ASSERT $value INSIDE ['pending', 'completed', 'failed', 'retrying'] PERMISSIONS FULL;

DEFINE INDEX idx_expires_at ON idp FIELDS expiresAt;
DEFINE INDEX idx_key_namespace ON idp FIELDS key, namespace UNIQUE;

DEFINE EVENT idp_cleanup ON idp WHEN expiresAt < time::now() THEN (DELETE $before);

-- ------------------------------
-- TABLE: organizations
-- ------------------------------

DEFINE TABLE organizations TYPE ANY SCHEMALESS PERMISSIONS NONE;

DEFINE FIELD adminDeleteEnabled ON organizations TYPE bool DEFAULT false PERMISSIONS FULL;
DEFINE FIELD authProviders ON organizations TYPE array PERMISSIONS FULL;
DEFINE FIELD authProviders[*] ON organizations TYPE object PERMISSIONS FULL;
DEFINE FIELD authProviders[*].provider ON organizations TYPE string PERMISSIONS FULL;
DEFINE FIELD authProviders[*].providerOrganizationId ON organizations TYPE string PERMISSIONS FULL;
DEFINE FIELD createdAt ON organizations TYPE datetime DEFAULT time::now() READONLY PERMISSIONS FULL;
DEFINE FIELD hasImage ON organizations TYPE bool DEFAULT false PERMISSIONS FULL;
DEFINE FIELD imageUrl ON organizations TYPE option<string> PERMISSIONS FULL;
DEFINE FIELD logoUrl ON organizations TYPE option<string> PERMISSIONS FULL;
DEFINE FIELD maxAllowedMemberships ON organizations TYPE int PERMISSIONS FULL;
DEFINE FIELD metadata ON organizations FLEXIBLE TYPE option<object> PERMISSIONS FULL;
DEFINE FIELD name ON organizations TYPE string PERMISSIONS FULL;
DEFINE FIELD primaryAuthProviderId ON organizations TYPE string PERMISSIONS FULL;
DEFINE FIELD privateMetadata ON organizations FLEXIBLE TYPE option<object> PERMISSIONS FULL;
DEFINE FIELD slug ON organizations TYPE string PERMISSIONS FULL;
DEFINE FIELD stripeCustomerAccountId ON organizations TYPE option<string> PERMISSIONS FULL;
DEFINE FIELD updatedAt ON organizations TYPE datetime VALUE time::now() PERMISSIONS FULL;

-- ------------------------------
-- TABLE: users
-- ------------------------------

DEFINE TABLE users TYPE ANY SCHEMALESS PERMISSIONS NONE;

DEFINE FIELD authProviders ON users TYPE array PERMISSIONS FULL;
DEFINE FIELD authProviders[*] ON users TYPE object PERMISSIONS FULL;
DEFINE FIELD authProviders[*].provider ON users TYPE string PERMISSIONS FULL;
DEFINE FIELD authProviders[*].providerUserId ON users TYPE string PERMISSIONS FULL;
DEFINE FIELD banned ON users TYPE bool DEFAULT false PERMISSIONS FULL;
DEFINE FIELD createdAt ON users TYPE datetime DEFAULT time::now() READONLY PERMISSIONS FULL;
DEFINE FIELD emailAddresses ON users TYPE array PERMISSIONS FULL;
DEFINE FIELD emailAddresses[*] ON users TYPE string ASSERT string::is::email($value) PERMISSIONS FULL;
DEFINE FIELD externalId ON users TYPE option<string> PERMISSIONS FULL;
DEFINE FIELD firstName ON users TYPE string PERMISSIONS FULL;
DEFINE FIELD lastName ON users TYPE string PERMISSIONS FULL;
DEFINE FIELD locked ON users TYPE bool DEFAULT false PERMISSIONS FULL;
DEFINE FIELD metadata ON users FLEXIBLE TYPE option<object> PERMISSIONS FULL;
DEFINE FIELD mfaEnabled ON users TYPE bool DEFAULT false PERMISSIONS FULL;
DEFINE FIELD phoneNumbers ON users TYPE array PERMISSIONS FULL;
DEFINE FIELD phoneNumbers[*] ON users TYPE string PERMISSIONS FULL;
DEFINE FIELD primaryAuthProviderId ON users TYPE string PERMISSIONS FULL;
DEFINE FIELD primaryEmail ON users TYPE string ASSERT string::is::email($value) PERMISSIONS FULL;
DEFINE FIELD primaryPhoneNumber ON users TYPE option<string> PERMISSIONS FULL;
DEFINE FIELD primaryWeb3Wallet ON users TYPE option<string> PERMISSIONS FULL;
DEFINE FIELD privateMetadata ON users FLEXIBLE TYPE option<object> PERMISSIONS FULL;
DEFINE FIELD profileImageUrl ON users TYPE option<string> PERMISSIONS FULL;
DEFINE FIELD unsafeMetadata ON users FLEXIBLE TYPE option<object> PERMISSIONS FULL;
DEFINE FIELD updatedAt ON users TYPE datetime VALUE time::now() PERMISSIONS FULL;
DEFINE FIELD username ON users TYPE option<string> PERMISSIONS FULL;
DEFINE FIELD web3Wallets ON users TYPE array PERMISSIONS FULL;
DEFINE FIELD web3Wallets[*] ON users TYPE string PERMISSIONS FULL;

DEFINE INDEX primaryEmail_idx ON users FIELDS primaryEmail UNIQUE;

-- ------------------------------
-- TABLE: subscriptions
-- ------------------------------

DEFINE TABLE subscriptions TYPE ANY SCHEMALESS PERMISSIONS NONE;

DEFINE FIELD cancelAtPeriodEnd ON subscriptions TYPE bool DEFAULT false PERMISSIONS FULL;
DEFINE FIELD createdAt ON subscriptions TYPE datetime DEFAULT time::now() READONLY PERMISSIONS FULL;
DEFINE FIELD currentPeriodEnd ON subscriptions TYPE datetime PERMISSIONS FULL;
DEFINE FIELD currentPeriodStart ON subscriptions TYPE datetime PERMISSIONS FULL;
DEFINE FIELD initiatedByUserId ON subscriptions TYPE option<record<users>> PERMISSIONS FULL;
DEFINE FIELD organizationId ON subscriptions TYPE record<organizations> PERMISSIONS FULL;
DEFINE FIELD planKey ON subscriptions TYPE option<string> PERMISSIONS FULL;
DEFINE FIELD provider ON subscriptions TYPE string PERMISSIONS FULL;
DEFINE FIELD providerSubscriptionData ON subscriptions FLEXIBLE TYPE object PERMISSIONS FULL;
DEFINE FIELD providerSubscriptionId ON subscriptions TYPE string PERMISSIONS FULL;
DEFINE FIELD status ON subscriptions TYPE string PERMISSIONS FULL;
DEFINE FIELD tokensPerPeriod ON subscriptions TYPE int PERMISSIONS FULL;
DEFINE FIELD updatedAt ON subscriptions TYPE datetime VALUE time::now() PERMISSIONS FULL;

DEFINE INDEX initiatedByUserId_idx ON subscriptions FIELDS initiatedByUserId;
DEFINE INDEX organizationId_idx ON subscriptions FIELDS organizationId;
DEFINE INDEX providerSubscriptionId_idx ON subscriptions FIELDS providerSubscriptionId UNIQUE;


-- ------------------------------
-- TABLE: heartbeats
-- ------------------------------

DEFINE TABLE heartbeats TYPE NORMAL SCHEMALESS PERMISSIONS NONE;

DEFINE FIELD started_at ON heartbeats TYPE datetime DEFAULT time::now() PERMISSIONS FOR select, create FULL, FOR update NONE;
DEFINE FIELD task_id ON heartbeats TYPE string PERMISSIONS FOR select, create FULL, FOR update NONE;
DEFINE FIELD timestamp ON heartbeats TYPE datetime PERMISSIONS FULL;


-- ------------------------------
-- TABLE: tasks
-- ------------------------------

DEFINE TABLE tasks TYPE NORMAL SCHEMALESS PERMISSIONS NONE;

DEFINE FIELD action ON tasks TYPE string ASSERT $value INSIDE ['start', 'stop', 'cancel', 'pause', 'resume', 'status', 'ack_cancel', 'ack_pause', 'ack_resume', 'ack_status'] PERMISSIONS FULL;
DEFINE FIELD payload ON tasks TYPE object PERMISSIONS FULL;
DEFINE FIELD status ON tasks TYPE string ASSERT $value INSIDE ['pending', 'running', 'cancelled', 'completed', 'failed'] PERMISSIONS FULL;
DEFINE FIELD task_id ON tasks TYPE string PERMISSIONS FOR select, create FULL, FOR update NONE;
DEFINE FIELD timestamp ON tasks TYPE datetime PERMISSIONS FULL;
