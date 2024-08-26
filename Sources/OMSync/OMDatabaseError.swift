//
//  File.swift
//  
//
//  Created by John Knowles on 7/17/24.
//

import Foundation
import GRDB

public enum OMDatabaseError: LocalizedError {
    case sql(resultCode: ResultCode)
    case invalidURL
    case failedMigration
    case failedInitialization
    
    public var errorDescription: String? {
        switch self {
        case .sql(resultCode: let code):
            return code.description
        case .invalidURL:
            return "URL is invalid"
        case .failedMigration:
            return "Migration failed"
        case .failedInitialization:
            return "Initialization failed"
        }
    }
}
