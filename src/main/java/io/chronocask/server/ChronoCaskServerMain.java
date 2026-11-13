package io.chronocask.server;

import io.chronocask.api.ChronoCask;
import io.chronocask.api.ChronoCaskConfig;
import io.chronocask.segment.DurabilityMode;

import java.nio.file.Path;

/**
 * Main command-line application launcher for ChronoCask Server.
 */
public final class ChronoCaskServerMain {

    private static final String BANNER = """
               ______ __                                  ______           __
              / ____// /_   _____ ____   ____   ____     / ____/____ _ _____/ /__
             / /    / __ \\ / ___// __ \\ / __ \\ / __ \\   / /    / __ `/ ___// //_/
            / /___ / / / // /   / /_/ // / / // /_/ /  / /___ / /_/ /(__  )/ ,<
            \\____//_/ /_//_/    \\____//_/ /_/ \\____/   \\____/ \\__,_//____//_/|_|
                                                         Standalone Server 0.1.0
            """;

    public static void main(String[] args) {
        System.out.println(BANNER);

        int port = 7379;
        String host = "0.0.0.0";
        Path dataDir = Path.of("./data");
        boolean offHeap = true;
        DurabilityMode durability = DurabilityMode.ASYNC;
        long maxSegmentSizeBytes = 64 * 1024 * 1024L;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port", "-p" -> {
                    if (i + 1 < args.length) port = Integer.parseInt(args[++i]);
                }
                case "--host", "-h" -> {
                    if (i + 1 < args.length) host = args[++i];
                }
                case "--dir", "-d" -> {
                    if (i + 1 < args.length) dataDir = Path.of(args[++i]);
                }
                case "--off-heap" -> {
                    if (i + 1 < args.length) offHeap = Boolean.parseBoolean(args[++i]);
                }
                case "--sync" -> durability = DurabilityMode.SYNC;
                case "--segment-size-mb" -> {
                    if (i + 1 < args.length) {
                        maxSegmentSizeBytes = Long.parseLong(args[++i]) * 1024 * 1024L;
                    }
                }
            }
        }

        try {
            ChronoCaskConfig config = ChronoCaskConfig.builder(dataDir)
                    .offHeapIndex(offHeap)
                    .defaultDurability(durability)
                    .maxSegmentSizeBytes(maxSegmentSizeBytes)
                    .build();

            System.out.printf("[ChronoCask] Opening storage engine at '%s' (off-heap=%b, durability=%s)%n",
                    dataDir.toAbsolutePath(), offHeap, durability);
            ChronoCask store = ChronoCask.open(config);

            ChronoCaskServer server = ChronoCaskServer.create(store, host, port);
            server.start();

            System.out.printf("[ChronoCask] Ready to accept Redis/RESP connections on %s:%d%n", host, server.getPort());
            System.out.println("[ChronoCask] Press Ctrl+C to terminate.");

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("[ChronoCask] Gracefully shutting down server...");
                server.stop();
                store.close();
                System.out.println("[ChronoCask] Storage cleanly flushed and closed.");
            }));

            // Keep main thread alive
            Thread.currentThread().join();

        } catch (Exception e) {
            System.err.println("[ChronoCask] Fatal error starting server: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
