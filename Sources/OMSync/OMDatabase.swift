//
//  File.swift
//  
//
//  Created by John Knowles on 7/17/24.
//

import Foundation
import GRDB


import Foundation
import GRDB
import os.log

class OMDatabase {
    /// Creates an `AppDatabase`, and makes sure the database schema
    /// is ready.
    ///
    /// - important: Create the `DatabaseWriter` with a configuration
    ///   returned by ``makeConfiguration(_:)``.
    init(_ dbWriter: any DatabaseWriter) throws {
        self.dbWriter = dbWriter
        try migrator.migrate(dbWriter)
//        try migrator.migrate(dbPool)
        if try dbWriter.read(migrator.hasBeenSuperseded) {
            // Database is too recent
            throw DatabaseError.failedMigration/* some error */
        }
    }

    /// Provides access to the database.
    ///
    /// Application can use a `DatabasePool`, while SwiftUI previews and tests
    /// can use a fast in-memory `DatabaseQueue`.
    ///
    /// See <https://swiftpackageindex.com/groue/grdb.swift/documentation/grdb/databaseconnections>
     let dbWriter: any DatabaseWriter
}

// MARK: - Database Configuration

extension OMDatabase {
    private static let sqlLogger = OSLog(subsystem: Bundle.main.bundleIdentifier!, category: "SQL")

    public static func makeConfiguration(_ base: Configuration = Configuration()) -> Configuration {
        var config = base

        // An opportunity to add required custom SQL functions or
        // collations, if needed:
        // config.prepareDatabase { db in
        //     db.add(function: ...)
        // }

        // Log SQL statements if the `SQL_TRACE` environment variable is set.
        // See <https://swiftpackageindex.com/groue/grdb.swift/documentation/grdb/database/trace(options:_:)>
        if ProcessInfo.processInfo.environment["SQL_TRACE"] != nil {
            config.prepareDatabase { db in
                db.trace {
                    // It's ok to log statements publicly. Sensitive
                    // information (statement arguments) are not logged
                    // unless config.publicStatementArguments is set
                    // (see below).
                    os_log("%{public}@", log: sqlLogger, type: .debug, String(describing: $0))
                }
            }
        }

#if DEBUG
        // Protect sensitive information by enabling verbose debugging in
        // DEBUG builds only.
        // See <https://swiftpackageindex.com/groue/grdb.swift/documentation/grdb/configuration/publicstatementarguments>
        config.publicStatementArguments = true
#endif

        return config
    }
}

// MARK: - Database Migrations

extension OMDatabase {
    private var migrator: DatabaseMigrator {
        var migrator = DatabaseMigrator()

#if DEBUG
        // Speed up development by nuking the database when migrations change
        // See <https://swiftpackageindex.com/groue/grdb.swift/documentation/grdb/migrations>
        migrator.eraseDatabaseOnSchemaChange = true
#endif

        migrator.registerMigration("createTables") { db in
            // Create a table
            // See <https://swiftpackageindex.com/groue/grdb.swift/documentation/grdb/databaseschema>
            
            try Stack.createTable(db)
            try Stack.createFTSTable(db)
            try Link.createTable(db)
            try Link.createFTSTable(db)

        }

        // Migrations for future application versions will be inserted here:
        // migrator.registerMigration(...) { db in
        //     ...
        // }

        return migrator
    }
}

// MARK: - Database Access: Writes
// The write methods execute invariant-preserving database transactions.

extension OMDatabase {
    /// A validation error that prevents some players from being saved into
    /// the database.
    enum DatabaseError: LocalizedError {
        case sql(resultCode: ResultCode)
        case invalidURL
        case failedMigration
        var errorDescription: String? {
            switch self {
            case .sql(resultCode: let code):
                return code.description
            case .invalidURL:
                return "URL is invalid"
            case .failedMigration:
                return "Migration failed"
            }
        }
    }
}




extension OMDatabase {
    /// Provides a read-only access to the database
    var reader: DatabaseReader {
        dbWriter
    }
}



extension OMDatabase {
    /// The database for the application
    static let shared = makeShared()
    
    private static func makeShared() -> OMDatabase {
        do {
            // Apply recommendations from
            // <https://swiftpackageindex.com/groue/grdb.swift/documentation/grdb/databaseconnections>
            //
            // Create the "Application Support/Database" directory if needed
            let databaseURL = try URL.getAppURL(for: "db.sqlite")
            NSLog("Database stored at \(databaseURL.path)")
            
//            let dbQueue = try DatabaseQueue(path: databaseURL.path,
//                                            configuration: DatabaseManager.makeConfiguration())
            let dbPool = try openDatabase(at: databaseURL)

//            // Create the AppDatabase
            let appDatabase = try AppDatabase(dbPool)
//            let appDatabase = try DatabaseManager(dbQueue)
                // do any setup here
            
            
            return appDatabase
        } catch {
            // Replace this implementation with code to handle the error appropriately.
            // fatalError() causes the application to generate a crash log and terminate.
            //
            // Typical reasons for an error here include:
            // * The parent directory cannot be created, or disallows writing.
            // * The database is not accessible, due to permissions or data protection when the device is locked.
            // * The device is out of space.
            // * The database could not be migrated to its latest schema version.
            // Check the error message to determine what the actual problem was.
            fatalError("Unresolved error \(error)")
        }
    }
}

extension AppDatabase {
    static func openSharedDatabase(at databaseURL: URL) throws -> DatabasePool {
        let coordinator = NSFileCoordinator(filePresenter: nil)
        var coordinatorError: NSError?
        var dbPool: DatabasePool?
        var dbError: Error?
        coordinator.coordinate(writingItemAt: databaseURL, options: .forMerging, error: &coordinatorError) { url in
            do {
                dbPool = try openDatabase(at: url)
            } catch {
                dbError = error
            }
        }
        if let error = dbError ?? coordinatorError {
            throw error
        }
        return dbPool!
    }


    private static func openDatabase(at databaseURL: URL) throws -> DatabasePool {
        var configuration = Configuration()
        configuration.prepareDatabase { db in
            // Activate the persistent WAL mode so that
            // read-only processes can access the database.
            //
            // See https://www.sqlite.org/walformat.html#operations_that_require_locks_and_which_locks_those_operations_use
            // and https://www.sqlite.org/c3ref/c_fcntl_begin_atomic_write.html#sqlitefcntlpersistwal
            if db.configuration.readonly == false {
                var flag: CInt = 1
                let code = withUnsafeMutablePointer(to: &flag) { flagP in
                    sqlite3_file_control(db.sqliteConnection, nil, SQLITE_FCNTL_PERSIST_WAL, flagP)
                }
                guard code == SQLITE_OK else {
                    throw DatabaseError.sql(resultCode: ResultCode(rawValue: code))
                }
            }
        }
        let dbPool = try DatabasePool(path: databaseURL.path, configuration: configuration)
        
        // Perform here other database setups, such as defining
        // the database schema with a DatabaseMigrator, and
        // checking if the application can open the file:
        
        
        return dbPool
    }
}


/*
 
        CRUD
 */
extension AppDatabase {
    
    @discardableResult
    func insertAll<T: MutablePersistableRecord>(_ records : [T])  throws -> [T] {
        return try dbWriter.write { db in
            var insertedRecords: [T] = []
            for record in records {
                let insert = try record
                    .inserted(db)
                insertedRecords.append(insert)
            }
            return insertedRecords
        }
    }
    
    
    @discardableResult
    func deleteAll(_ record: TableRecord.Type,  ids: [Int64]) throws -> Int {
        try dbWriter.write { db in
            try record
                .deleteAll(db, keys: ids)
        }
    }
    
    @discardableResult
    func updateAll(_ record: (some TableRecord).Type, ids: [Int64], assignments:[(String, (any SQLExpressible)?)]) throws -> Int {
        let assignments = assignments.map {
            Column($0.0).set(to: $0.1)
            
        }
        
        return try dbWriter.write { db in
            try record
                .filter(keys: ids)
                .updateAll(db, assignments)
        }
    }
}

extension URL {
    static func getAppURL(for file: String) throws -> URL {
        let fileManager = FileManager.default
        let appSupportURL = try fileManager.url(
            for: .applicationSupportDirectory, in: .userDomainMask,
            appropriateFor: nil, create: true)
        let directoryURL = appSupportURL.appendingPathComponent("support", isDirectory: true)
        
        // Create the database folder if needed
        try fileManager.createDirectory(at: directoryURL, withIntermediateDirectories: true)
        
        // Open or create the database
        let databaseURL = directoryURL.appendingPathComponent(file)
        return databaseURL
    }
}
