//
//  File.swift
//  
//
//  Created by John Knowles on 7/17/24.
//

import Foundation
import GRDB
import os.log

public class OMLocalDatabase: OMDatabase {
    public init(name: String? = nil) throws {
         let databaseURL = try URL.getAppURL(for: "\(name ?? "").db.sqlite")
//            let dbQueue = try DatabaseQueue(path: databaseURL.path,
//                                            configuration: DatabaseManager.makeConfiguration())
//         self.writer = try openDatabase(at: databaseURL)
         
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
                     throw OMDatabaseError.sql(resultCode: ResultCode(rawValue: code))
                 }
             }
             
             // An opportunity to add required custom SQL functions or
             // collations, if needed:
             
             //  db.add(function: ...)
         }
         
         self.writer = try DatabasePool(path: databaseURL.path, 
                                        configuration: configuration)

         
//        self.writer = dbPool
        try migrator.migrate(writer)
         
        if try writer.read(migrator.hasBeenSuperseded) {
            // Database is too recent
            throw OMDatabaseError.failedMigration/* some error */
        }
    }

    public let writer: any DatabaseWriter
    
    public var migrator: DatabaseMigrator {
        var migrator = DatabaseMigrator()

#if DEBUG
        // Speed up development by nuking the database when migrations change
        // See <https://swiftpackageindex.com/groue/grdb.swift/documentation/grdb/migrations>
        migrator.eraseDatabaseOnSchemaChange = true
#endif

        migrator.registerMigration("createTables") { db in
                // try Record.crreate...
        }

        // Migrations for future application versions will be inserted here:
        // migrator.registerMigration(...) { db in
        //     ...
        // }

        return migrator
    }
    
    public let logger = OSLog(subsystem: Bundle.main.bundleIdentifier!, category: "SQL")


}




