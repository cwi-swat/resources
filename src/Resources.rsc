module Resources

import Type;
import Exception;

data RuntimeException = unexpectedScheme(str expectedScheme, str providedScheme, loc path);

alias QueryParam = tuple[str paramName, Symbol paramType, bool paramRequired];
alias QueryParams = list[QueryParam];

