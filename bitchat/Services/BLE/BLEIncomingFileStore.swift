import BitLogger
import BitFoundation
import Foundation

struct BLEIncomingFileStore: @unchecked Sendable {
    struct PrivateMediaDeletionReservation: Sendable {
        fileprivate let id: UUID
    }

    private final class PayloadCoordination: @unchecked Sendable {
        let lock = NSLock()
        var pendingDeliveryPaths: Set<String> = []
        var deletionReservations: [UUID: Set<String>] = [:]
    }

    private static let defaultQuotaBytes: Int64 = 100 * 1024 * 1024

    /// Name prefix of in-flight live voice captures (progressively written by
    /// `ChatLiveVoiceCoordinator`). Quota eviction skips them by pattern —
    /// deleting one mid-stream unlinks the inode under an open `FileHandle`
    /// and kills playback — and the coordinator's startup sweep deletes any
    /// orphans a previous session left behind.
    static let liveCapturePrefix = "voice_live_"

    /// Exposed so callers that write progressively into the store's
    /// directories (live voice captures) share the same file manager.
    let fileManager: FileManager
    private let baseDirectory: URL?
    private let dateProvider: () -> Date
    private let quotaBytes: Int64
    private let privateMediaReceipts: BLEPrivateMediaReceiptStore
    private let payloadCoordination: PayloadCoordination

    init(
        fileManager: FileManager = .default,
        baseDirectory: URL? = nil,
        dateProvider: @escaping () -> Date = Date.init,
        quotaBytes: Int64 = Self.defaultQuotaBytes
    ) {
        self.fileManager = fileManager
        self.baseDirectory = baseDirectory
        self.dateProvider = dateProvider
        self.quotaBytes = max(0, quotaBytes)
        self.privateMediaReceipts = BLEPrivateMediaReceiptStore(
            fileManager: fileManager,
            baseDirectory: baseDirectory,
            now: dateProvider
        )
        self.payloadCoordination = PayloadCoordination()
    }

    /// Resolves (and creates) an incoming-media directory for callers that
    /// write progressively instead of via `save` (live voice captures).
    func incomingDirectory(subdirectory: String) throws -> URL {
        let directory = try filesDirectory().appendingPathComponent(subdirectory, isDirectory: true)
        try fileManager.createDirectory(at: directory, withIntermediateDirectories: true, attributes: nil)
        return directory
    }

    func save(
        data: Data,
        preferredName: String?,
        subdirectory: String,
        fallbackExtension: String?,
        defaultPrefix: String
    ) -> URL? {
        payloadCoordination.lock.lock()
        defer { payloadCoordination.lock.unlock() }

        do {
            let base = try filesDirectory().appendingPathComponent(subdirectory, isDirectory: true)
            try fileManager.createDirectory(at: base, withIntermediateDirectories: true, attributes: nil)
            let sanitized = sanitizedFileName(
                preferredName,
                defaultName: "\(defaultPrefix)_\(Self.timestampString(from: dateProvider()))",
                fallbackExtension: fallbackExtension
            )
            let reservedPaths = privateMediaReceipts.reservedPayloadPaths()
            let deletionPaths = payloadCoordination
                .deletionReservations.values.reduce(into: Set<String>()) {
                    $0.formUnion($1)
                }
            let allocationReservations = deletionPaths.union(
                payloadCoordination.pendingDeliveryPaths
            )
            let destination = uniqueFileURL(
                in: base,
                fileName: sanitized,
                reservedPaths: (reservedPaths ?? []).union(
                    allocationReservations
                ),
                forceRandomizedName: reservedPaths == nil
            )
            try data.write(to: destination, options: .atomic)
            payloadCoordination.pendingDeliveryPaths.insert(
                destination.standardizedFileURL.path
            )
            return destination
        } catch {
            SecureLogger.error("❌ Failed to persist incoming media: \(error)", category: .session)
            return nil
        }
    }

    func privateMediaReceiptState(
        messageID: String
    ) -> BLEPrivateMediaReceiptState {
        privateMediaReceipts.state(for: messageID)
    }

    func commitPrivateMediaFile(
        messageID: String,
        storedURL: URL
    ) -> Bool {
        privateMediaReceipts.commitAccepted(
            messageID: messageID,
            storedURL: storedURL
        )
    }

    /// Reserves every receipt/UI path before the asynchronous deletion
    /// barrier. Allocation and reservation share one lock, so either an
    /// in-flight raw arrival is observed and deletion fails closed, or the
    /// arrival is forced onto a different filename.
    func reservePrivateMediaDeletion(
        messageIDs: [String],
        payloadRelativePaths: [String: String]
    ) -> PrivateMediaDeletionReservation? {
        payloadCoordination.lock.lock()
        defer { payloadCoordination.lock.unlock() }

        guard let paths = privateMediaReceipts
            .prospectiveDeletionPayloadPaths(
                messageIDs: messageIDs,
                payloadRelativePaths: payloadRelativePaths
            ),
        paths.isDisjoint(
            with: payloadCoordination.pendingDeliveryPaths
        ) else {
            return nil
        }

        let reservation = PrivateMediaDeletionReservation(id: UUID())
        payloadCoordination.deletionReservations[reservation.id] = paths
        return reservation
    }

    func commitPrivateMediaDeletion(
        reservation: PrivateMediaDeletionReservation,
        messageIDs: [String],
        payloadRelativePaths: [String: String],
        protectedPayloadRelativePaths: Set<String>
    ) -> Bool {
        payloadCoordination.lock.lock()
        defer {
            payloadCoordination.deletionReservations.removeValue(
                forKey: reservation.id
            )
            payloadCoordination.lock.unlock()
        }
        guard payloadCoordination.deletionReservations[reservation.id] != nil
        else {
            return false
        }
        return privateMediaReceipts.recordDeleted(
            messageIDs: messageIDs,
            payloadRelativePaths: payloadRelativePaths,
            protectedPayloadRelativePaths: protectedPayloadRelativePaths
        )
    }

    /// Releases the short window between disk save and synchronous
    /// conversation insertion. Before this callback, a deletion transaction
    /// may not infer ownership from a stale bubble that names the same path.
    func finishIncomingFileDelivery(at storedURL: URL) {
        payloadCoordination.lock.lock()
        defer { payloadCoordination.lock.unlock() }
        payloadCoordination.pendingDeliveryPaths.remove(
            storedURL.standardizedFileURL.path
        )
    }

    /// Best-effort rollback for a payload whose durable receipt commit failed.
    func removeIncomingFile(at storedURL: URL) {
        payloadCoordination.lock.lock()
        defer { payloadCoordination.lock.unlock() }
        payloadCoordination.pendingDeliveryPaths.remove(
            storedURL.standardizedFileURL.path
        )
        guard isURLInsideFilesDirectory(storedURL) else { return }
        do {
            try fileManager.removeItem(at: storedURL)
        } catch {
            SecureLogger.warning(
                "⚠️ Failed to roll back uncommitted incoming media: \(error)",
                category: .session
            )
        }
    }

    /// Frees least-recently-modified incoming files until `reservingBytes`
    /// fits under the quota. Files named `voice_live_*` (in-flight live
    /// captures) are never evicted regardless of who triggers enforcement —
    /// a finalized transfer can arrive at quota while a burst is still
    /// streaming — but they still count toward usage.
    func enforceQuota(reservingBytes: Int) {
        payloadCoordination.lock.lock()
        defer { payloadCoordination.lock.unlock() }

        do {
            let base = try filesDirectory()
            let incomingDirs = [
                base.appendingPathComponent("voicenotes/incoming", isDirectory: true),
                base.appendingPathComponent("images/incoming", isDirectory: true),
                base.appendingPathComponent("files/incoming", isDirectory: true)
            ]
            var allFiles: [(url: URL, size: Int64, modified: Date)] = []

            for dir in incomingDirs where fileManager.fileExists(atPath: dir.path) {
                guard let contents = try? fileManager.contentsOfDirectory(
                    at: dir,
                    includingPropertiesForKeys: [.fileSizeKey, .contentModificationDateKey],
                    options: [.skipsHiddenFiles]
                ) else { continue }

                for fileURL in contents {
                    guard let attrs = try? fileURL.resourceValues(forKeys: [.fileSizeKey, .contentModificationDateKey]),
                          let size = attrs.fileSize,
                          let modified = attrs.contentModificationDate else { continue }
                    allFiles.append((url: fileURL, size: Int64(size), modified: modified))
                }
            }

            let currentUsage = allFiles.reduce(0) { $0 + $1.size }
            let targetUsage = quotaBytes - Int64(reservingBytes)
            guard currentUsage > targetUsage else { return }

            let needToFree = currentUsage - targetUsage
            let activeDeletionPaths = payloadCoordination
                .deletionReservations.values.reduce(into: Set<String>()) {
                    $0.formUnion($1)
                }
            let protectedPaths = activeDeletionPaths.union(
                payloadCoordination.pendingDeliveryPaths
            )
            var freedSpace: Int64 = 0
            for file in allFiles.sorted(by: { $0.modified < $1.modified }) {
                guard freedSpace < needToFree else { break }
                guard !file.url.lastPathComponent.hasPrefix(Self.liveCapturePrefix) else { continue }
                guard !protectedPaths.contains(
                    file.url.standardizedFileURL.path
                ) else {
                    continue
                }
                do {
                    try fileManager.removeItem(at: file.url)
                    freedSpace += file.size
                    SecureLogger.debug("🗑️ BCH-01-002: Deleted old incoming file to free space: \(file.url.lastPathComponent)", category: .security)
                } catch {
                    SecureLogger.warning("⚠️ Failed to delete old file for quota: \(error)", category: .security)
                }
            }

            if freedSpace > 0 {
                SecureLogger.info("📊 BCH-01-002: Freed \(ByteCountFormatter.string(fromByteCount: freedSpace, countStyle: .file)) to stay within incoming files quota", category: .security)
            }
        } catch {
            SecureLogger.warning("⚠️ Could not enforce storage quota: \(error)", category: .security)
        }
    }

    private func filesDirectory() throws -> URL {
        let root = try baseDirectory ?? fileManager.url(
            for: .applicationSupportDirectory,
            in: .userDomainMask,
            appropriateFor: nil,
            create: true
        )
        let filesDir = root.appendingPathComponent("files", isDirectory: true)
        try fileManager.createDirectory(at: filesDir, withIntermediateDirectories: true, attributes: nil)
        return filesDir
    }

    private func isURLInsideFilesDirectory(_ url: URL) -> Bool {
        guard let filesDirectory = try? filesDirectory().standardizedFileURL else {
            return false
        }
        return url.standardizedFileURL.path.hasPrefix(filesDirectory.path + "/")
    }

    private func sanitizedFileName(_ name: String?, defaultName: String, fallbackExtension: String?) -> String {
        var candidate = (name ?? "")
            .replacingOccurrences(of: "\0", with: "")
            .precomposedStringWithCanonicalMapping
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "\\", with: "_")

        let invalid = CharacterSet(charactersIn: "<>:\"|?*\0").union(.controlCharacters)
        candidate = candidate.components(separatedBy: invalid).joined(separator: "_").trimmed
        if candidate.isEmpty { candidate = defaultName }
        if candidate.hasPrefix(".") { candidate = "_" + candidate }

        if candidate.count > 120 {
            let ext = (candidate as NSString).pathExtension
            let base = (candidate as NSString).deletingPathExtension
            candidate = ext.isEmpty
                ? String(candidate.prefix(120))
                : String(base.prefix(max(10, 120 - ext.count - 1))) + "." + ext
        }

        if let fallbackExtension, (candidate as NSString).pathExtension.isEmpty {
            candidate += ".\(fallbackExtension)"
        }

        return candidate.isEmpty ? defaultName : candidate
    }

    private func uniqueFileURL(
        in directory: URL,
        fileName: String,
        reservedPaths: Set<String>,
        forceRandomizedName: Bool
    ) -> URL {
        let directoryPath = directory.standardizedFileURL.path
        func isInsideDirectory(_ url: URL) -> Bool {
            url.standardizedFileURL.path.hasPrefix(directoryPath + "/")
        }
        func isAvailable(_ url: URL) -> Bool {
            !reservedPaths.contains(url.standardizedFileURL.path)
                && !fileManager.fileExists(atPath: url.path)
        }

        var candidate = directory.appendingPathComponent(fileName)
        guard isInsideDirectory(candidate) else {
            SecureLogger.warning("⚠️ Path traversal blocked: \(fileName)", category: .security)
            return directory.appendingPathComponent("blocked_\(UUID().uuidString)")
        }

        let baseName = (fileName as NSString).deletingPathExtension
        let ext = (fileName as NSString).pathExtension
        if forceRandomizedName {
            let suffix = UUID().uuidString
            let randomizedName = ext.isEmpty
                ? "\(baseName)_\(suffix)"
                : "\(baseName)_\(suffix).\(ext)"
            return directory.appendingPathComponent(randomizedName)
        }

        if isAvailable(candidate) {
            return candidate
        }

        for counter in 1..<100 {
            let newName = ext.isEmpty ? "\(baseName) (\(counter))" : "\(baseName) (\(counter)).\(ext)"
            candidate = directory.appendingPathComponent(newName)
            guard isInsideDirectory(candidate) else {
                return directory.appendingPathComponent("blocked_\(UUID().uuidString)")
            }
            if isAvailable(candidate) {
                return candidate
            }
        }

        return directory.appendingPathComponent("\(baseName)_\(UUID().uuidString).\(ext.isEmpty ? "dat" : ext)")
    }

    private static func timestampString(from date: Date) -> String {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyyMMdd_HHmmss"
        return formatter.string(from: date)
    }
}
