# Progress

This document tracks what works, what's left to build, current status, known issues, and the evolution of project decisions.

## What Works
- Configuration reading from YAML files
- Spark session initialization
- Database connector framework for multiple database types
- Schema mapping and Spark schema creation
- PostgreSQL and MySQL connection support

## What's Left
- Oracle database connection testing and validation
- Error handling improvements for database connection failures
- Connection pooling and retry mechanisms
- Performance optimization for large datasets

## Current Status
- Oracle connection issue identified and partially resolved
- Updated JDBC URL format from SID to Service Name format
- Ready for testing with the updated connection configuration

## Known Issues
- Oracle ORA-12505 error: SID 'orclpdb' not registered with listener
- May need to verify correct service name vs SID configuration
- Database connection format sensitivity for Oracle databases

## Evolution of Decisions
- Initially used SID format for Oracle connections
- Switched to Service Name format based on Oracle 12c+ best practices
- Added special handling for Oracle connections in databaseConnector.py
- Prepared fallback options for different Oracle connection scenarios 