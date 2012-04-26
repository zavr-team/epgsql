-record(column,    {name, type, size, modifier, format}).
-record(statement, {name, columns, types}).

-record(error,  {severity, code, message, extra}).

-define(DEFAULT_CONNECT_TIMEOUT, 5000).
