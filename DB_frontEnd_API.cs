using Microsoft.AspNetCore.Mvc;
using System.Text.RegularExpressions;

namespace ValorantApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class SqlController : ControllerBase
    {
        [HttpPost("generate")]
        public IActionResult GenerateSql([FromBody] SqlRequest request)
        {
            try
            {
                var parts = request.Query.Split('|');
                if (parts.Length != 3)
                    return BadRequest("Query must have 3 parts: ACTION|Table|key=value,...");

                var action = parts[0].Trim().ToUpper();
                var table = parts[1].Trim();
                var kvString = parts[2].Trim();

                var kvPairs = ParseKeyValuePairs(kvString);

                string sql = action switch
                {
                    "INSERT" => GenerateInsert(table, kvPairs),
                    "UPDATE" => GenerateUpdate(table, kvPairs),
                    "DELETE" => GenerateDelete(table, kvPairs),
                    _ => throw new Exception($"Unsupported action: {action}")
                };

                return Ok(new { sql });
            }
            catch (Exception ex)
            {
                return BadRequest(new { error = ex.Message });
            }
        }

        private Dictionary<string, string> ParseKeyValuePairs(string kvString)
        {
            var kvPairs = new Dictionary<string, string>();
            var pairs = kvString.Split(',');

            foreach (var pair in pairs)
            {
                var split = pair.Split('=');
                if (split.Length != 2)
                    throw new Exception($"Invalid pair: {pair}");

                kvPairs[split[0].Trim()] = split[1].Trim();
            }

            return kvPairs;
        }

        private string FormatValue(string value)
        {
            return Regex.IsMatch(value, @"^\d+(\.\d+)?$") ? value : $"'{value}'";
        }

        private string GenerateInsert(string table, Dictionary<string, string> kv)
        {
            var columns = string.Join(", ", kv.Keys);
            var values = string.Join(", ", kv.Values.Select(FormatValue));
            return $"INSERT INTO {table} ({columns}) VALUES ({values});";
        }

        private string GenerateUpdate(string table, Dictionary<string, string> kv)
        {
            if (!kv.ContainsKey("id"))
                throw new Exception("UPDATE requires 'id' as primary key");

            var id = kv["id"];
            kv.Remove("id");

            var setClause = string.Join(", ", kv.Select(kv => $"{kv.Key}={FormatValue(kv.Value)}"));
            return $"UPDATE {table} SET {setClause} WHERE id={id};";
        }

        private string GenerateDelete(string table, Dictionary<string, string> kv)
        {
            if (!kv.ContainsKey("id"))
                throw new Exception("DELETE requires 'id' as primary key");

            return $"DELETE FROM {table} WHERE id={kv["id"]};";
        }
    }

    public class SqlRequest
    {
        public string Query { get; set; }
    }
}