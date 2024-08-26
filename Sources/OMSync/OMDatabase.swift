//
//  File.swift
//  
//
//  Created by John Knowles on 7/17/24.
//

import Foundation
import GRDB
import os.log

public protocol OMDatabase {
    var writer: any DatabaseWriter { get }
    var reader: DatabaseReader { get }
    var migrator: DatabaseMigrator { get }
    var logger: OSLog { get }
}





public extension OMDatabase {
    public var reader: DatabaseReader {
        writer
    }
    


    public func openSharedDatabase(at databaseURL: URL, configuration: inout Configuration) throws -> DatabasePool {
        let coordinator = NSFileCoordinator(filePresenter: nil)
        var coordinatorError: NSError?
        var dbPool: DatabasePool?
        var dbError: Error?
        coordinator.coordinate(writingItemAt: databaseURL, options: .forMerging, error: &coordinatorError) { url in
            do {
                dbPool = try openDatabase(at: url, configuration: &configuration)
            } catch {
                dbError = error
            }
        }
        if let error = dbError ?? coordinatorError {
            throw error
        }
        return dbPool!
    }


    func openDatabase(at databaseURL: URL, configuration: inout Configuration) throws -> DatabasePool {        configuration.prepareDatabase { db in
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

extension MutablePersistableRecord {
    @discardableResult
    func insertAll(_ records : [Self], writer: DatabaseWriter)  throws -> [Self] {
        return try writer.write { db in
            var insertedRecords: [Self] = []
            for record in records {
                let insert = try record
                    .inserted(db)
                insertedRecords.append(insert)
            }
            return insertedRecords
        }
    }
}



extension OMDatabase {
//    
//    @discardableResult
//    func insertAll<T: MutablePersistableRecord>(_ records : [T])  throws -> [T] {
//        return try dbWriter.write { db in
//            var insertedRecords: [T] = []
//            for record in records {
//                let insert = try record
//                    .inserted(db)
//                insertedRecords.append(insert)
//            }
//            return insertedRecords
//        }
//    }
//    
//    
//    @discardableResult
//    func deleteAll(_ record: TableRecord.Type,  ids: [Int64]) throws -> Int {
//        try dbWriter.write { db in
//            try record
//                .deleteAll(db, keys: ids)
//        }
//    }
//    
//    @discardableResult
//    func updateAll(_ record: (some TableRecord).Type, ids: [Int64], assignments:[(String, (any SQLExpressible)?)]) throws -> Int {
//        let assignments = assignments.map {
//            Column($0.0).set(to: $0.1)
//            
//        }
//        
//        return try dbWriter.write { db in
//            try record
//                .filter(keys: ids)
//                .updateAll(db, assignments)
//        }
//    }
}

public extension URL {
    public static func getAppURL(for file: String) throws -> URL {
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
    
    static func getSharedURL(for file: String, groupIdentifier: String) throws -> URL {
           let fileManager = FileManager.default
           guard let sharedFolder: URL = fileManager.containerURL(forSecurityApplicationGroupIdentifier: groupIdentifier) else {
               throw OMDatabaseError.invalidURL
           }

   //        let appSupportURL = try fileManager.url(
   //            for: .applicationSupportDirectory, in: .userDomainMask,
   //            appropriateFor: nil, create: true)
           let directoryURL = sharedFolder.appendingPathComponent("support", isDirectory: true)
           
           // Create the database folder if needed
           try fileManager.createDirectory(at: directoryURL, withIntermediateDirectories: true)
           
           // Open or create the database
           let databaseURL = directoryURL.appendingPathComponent(file)
           return databaseURL
       }

}
