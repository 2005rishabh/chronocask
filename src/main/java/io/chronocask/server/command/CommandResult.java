package io.chronocask.server.command;

import io.chronocask.server.resp.RespValue;
import java.util.Objects;

/**
 * Result of executing a command, indicating the RESP response and whether
 * the client connection should be closed (e.g. on QUIT).
 */
public record CommandResult(RespValue response, boolean closeConnection) {

    public CommandResult {
        Objects.requireNonNull(response, "response cannot be null");
    }

    public static CommandResult reply(RespValue response) {
        return new CommandResult(response, false);
    }

    public static CommandResult quit() {
        return new CommandResult(RespValue.OK, true);
    }
}
