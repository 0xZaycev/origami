local function log(...)
    local message = "";

    for _, msg in pairs(arg) do
        if not _ == "n" then
            message = message .. " " .. msg;
        end;
    end;

    local time = redis.call("time");

    redis.call("publish", "log", "[" .. time[1] .. "." .. time[2] .. "] " .. message);
end;
