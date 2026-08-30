# Changelog

All notable changes to this project will be documented in this file.

## [0.2.0] - 2026-08-30

### Bug Fixes

- *(rpc)* Locate records with the primary key map sent by the host by @Robbyfuu
- *(rpc)* Keep the BSON type a field already holds when saving grid edits by @Robbyfuu
- *(rpc)* Answer unknown methods with the standard method-not-found wording by @Robbyfuu

### Features

- *(query)* Run write commands: insert, update, replace, delete by @Robbyfuu
- *(query)* Run collection and index management: createCollection, createIndex, dropIndex, drop, DROP TABLE by @Robbyfuu
- *(query)* Keep the index name and uniqueness chosen by the user by @Robbyfuu

### Documentation

- Document the write commands and the primary key contract by @Robbyfuu

## [0.1.1] - 2026-08-04

### Features

- *(manifest)* Add MongoDB icon by @Robbyfuu
## [0.1.0] - 2026-08-04

### Bug Fixes

- *(manifest)* Satisfy the Tabularium driver-kind contract by @NewtTheWolf
- *(ci)* Include hidden .tabularium file in manifest artifact upload by @debba
- *(manifest)* Rename plugin slug to mongodb-atlas by @debba

### Documentation

- Fix Discord shields.io badge server id by @debba

### Features

- *(mongodb)* Add initial plugin implementation and release pipeline by @debba
- *(mongodb)* Support Atlas and full connection URIs by @Robbyfuu
- Migrate to the .tabularium manifest for the Tabularium registry by @NewtTheWolf

### Miscellaneous

- Update repository references to tabularis-mongodb-plugin by @debba
- Switch Discord invite URL to discord.com/invite/K2hmhfHRSt by @debba

