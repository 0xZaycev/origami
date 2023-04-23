local function log(...)
    local message = "";

    for _, msg in pairs(arg) do
        message = message .. " " .. msg;
    end;

    local time = redis.call("time");

    redis.call("publish", "log", "[" .. time[1] .. time[2] .. "] " .. message);
end;
