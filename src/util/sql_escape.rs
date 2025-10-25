use mysql_common::value::Value;

/// Convert a MySQL value to its SQL literal representation
pub fn sql_literal_from_value(v: &Value) -> String {
    match v {
        Value::NULL => "NULL".to_string(),
        
        Value::Bytes(bytes) => {
            // For binary data, use hex literal: 0x...
            if bytes.is_empty() {
                "''".to_string()
            } else if is_likely_text(bytes) {
                // If it looks like text, escape it as a string
                escape_string(bytes)
            } else {
                // Otherwise use hex notation
                format!("0x{}", hex::encode(bytes))
            }
        }
        
        Value::Int(i) => i.to_string(),
        Value::UInt(u) => u.to_string(),
        Value::Float(f) => {
            if f.is_nan() {
                "'NaN'".to_string()
            } else if f.is_infinite() {
                if f.is_sign_positive() {
                    "'Infinity'".to_string()
                } else {
                    "'-Infinity'".to_string()
                }
            } else {
                f.to_string()
            }
        }
        Value::Double(d) => {
            if d.is_nan() {
                "'NaN'".to_string()
            } else if d.is_infinite() {
                if d.is_sign_positive() {
                    "'Infinity'".to_string()
                } else {
                    "'-Infinity'".to_string()
                }
            } else {
                d.to_string()
            }
        }
        
        Value::Date(year, month, day, hour, minute, second, micro) => {
            if *hour == 0 && *minute == 0 && *second == 0 && *micro == 0 {
                format!("'{:04}-{:02}-{:02}'", year, month, day)
            } else if *micro == 0 {
                format!("'{:04}-{:02}-{:02} {:02}:{:02}:{:02}'", 
                    year, month, day, hour, minute, second)
            } else {
                format!("'{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}'", 
                    year, month, day, hour, minute, second, micro)
            }
        }
        
        Value::Time(neg, days, hours, minutes, seconds, micros) => {
            let sign = if *neg { "-" } else { "" };
            let total_hours = *days * 24 + *hours as u32;
            if *micros == 0 {
                format!("'{}{:02}:{:02}:{:02}'", sign, total_hours, minutes, seconds)
            } else {
                format!("'{}{:02}:{:02}:{:02}.{:06}'", sign, total_hours, minutes, seconds, micros)
            }
        }
    }
}

/// Escape a string value for SQL (handles quotes, backslashes, etc.)
fn escape_string(bytes: &[u8]) -> String {
    let s = String::from_utf8_lossy(bytes);
    let mut result = String::with_capacity(s.len() + 20);
    result.push('\'');
    
    for ch in s.chars() {
        match ch {
            '\'' => result.push_str("''"),  // SQL standard: double single quotes
            '\\' => result.push_str("\\\\"), // Escape backslash
            '\0' => result.push_str("\\0"),  // Null byte
            '\n' => result.push_str("\\n"),  // Newline
            '\r' => result.push_str("\\r"),  // Carriage return
            '\t' => result.push_str("\\t"),  // Tab
            _ => result.push(ch),
        }
    }
    
    result.push('\'');
    result
}

/// Check if bytes are likely UTF-8 text (heuristic)
fn is_likely_text(bytes: &[u8]) -> bool {
    // Try to validate as UTF-8
    if std::str::from_utf8(bytes).is_err() {
        return false;
    }
    
    // Check for high proportion of printable characters
    let printable_count = bytes.iter()
        .filter(|&&b| b >= 32 && b < 127 || b == b'\n' || b == b'\r' || b == b'\t')
        .count();
    
    // If more than 90% printable, treat as text
    printable_count * 10 >= bytes.len() * 9
}

/// Escape an identifier (table name, column name) with backticks
pub fn escape_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Generate a multi-row INSERT statement
pub fn generate_insert_statement(
    table: &str,
    columns: &[String],
    rows: &[Vec<Value>],
) -> String {
    let mut sql = String::with_capacity(1024 * rows.len());
    
    sql.push_str("INSERT INTO ");
    sql.push_str(&escape_identifier(table));
    sql.push_str(" (");
    
    for (i, col) in columns.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&escape_identifier(col));
    }
    
    sql.push_str(") VALUES ");
    
    for (row_idx, row) in rows.iter().enumerate() {
        if row_idx > 0 {
            sql.push_str(", ");
        }
        sql.push('(');
        
        for (col_idx, value) in row.iter().enumerate() {
            if col_idx > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&sql_literal_from_value(value));
        }
        
        sql.push(')');
    }
    
    sql.push(';');
    sql
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_string() {
        let bytes = b"Hello 'World'";
        assert_eq!(escape_string(bytes), "'Hello ''World'''");
        
        let bytes = b"Line1\nLine2";
        assert_eq!(escape_string(bytes), "'Line1\\nLine2'");
    }

    #[test]
    fn test_escape_identifier() {
        assert_eq!(escape_identifier("my_table"), "`my_table`");
        assert_eq!(escape_identifier("my`table"), "`my``table`");
    }

    #[test]
    fn test_sql_literal_null() {
        assert_eq!(sql_literal_from_value(&Value::NULL), "NULL");
    }

    #[test]
    fn test_sql_literal_int() {
        assert_eq!(sql_literal_from_value(&Value::Int(42)), "42");
        assert_eq!(sql_literal_from_value(&Value::Int(-42)), "-42");
    }

    #[test]
    fn test_sql_literal_string() {
        let bytes = Value::Bytes(b"hello".to_vec());
        assert_eq!(sql_literal_from_value(&bytes), "'hello'");
    }
}
