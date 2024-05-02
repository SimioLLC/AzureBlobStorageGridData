using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace AzureBlobStorageGridData
{
    // See https://stackoverflow.com/questions/1869567/c-sharp-regex-how-can-i-replace-tokens-with-strings-generated-at-run-time
    static class TokenReplacement
    {
        const string TokenMatchRegexString = @"\${([^}]+)}";
        static readonly Regex TokenReplacementRegex = new Regex(TokenMatchRegexString, RegexOptions.Compiled);

        /// <summary>
        /// Replaces tokens in the given string in the form ${token}
        /// </summary>
        /// <param name="originalString">The original string with tokens to replace</param>
        /// <param name="tokenReplacements">The name-value pairs for tokens</param>
        /// <returns>A new string with the tokens replaced with the given values</returns>
        public static string ResolveString(string originalString,string password)
        {
            if (originalString == null)
                return null;

            if (originalString == String.Empty)
                return String.Empty;

            return TokenReplacementRegex.Replace(originalString, match =>
            {
                var token = match.Groups[1].Value;
                if (password != null && token.StartsWith("password", StringComparison.OrdinalIgnoreCase))
                {
                    var passwordTokenReplacement = new Dictionary<string, string>()
                    {
                        ["password"] = password
                    };
                    if (passwordTokenReplacement.TryGetValue("password", out var passwordValue))
                    {
                        return passwordValue;
                    }
                }
                if (token == "password") return "${password}";
                else return String.Empty;

            });
        }
    }
}
