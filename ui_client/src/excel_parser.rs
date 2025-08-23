use crate::excel_formulas::*;

pub fn parse_command(text: String, cells: Vec<Vec<String>>) -> Result<String, String> {
    let text = text.trim();

    if !text.starts_with('=') {
        return Ok(text.to_string()); // si no es una formula devuelvo el string tal y como esta
    }

    // separo comando y argumentos: =SUMA(B1:B2) -> "SUMA", "B1:B2"
    let open_paren = &text[1..].find('(');
    let close_paren = &text[1..].find(')');

    if open_paren.is_none() || close_paren.is_none() {
        return Err("ERROR: formato inválido".to_string());
    }

    let cmd = &&text[1..][..open_paren.unwrap()];
    let args_str = &&text[1..][open_paren.unwrap() + 1..close_paren.unwrap()];

    if args_str.contains(',') {
        let args: Vec<&str> = args_str.split(',').map(|s| s.trim()).collect();
        if args.len() != 2 {
            return Err("ERROR: se esperaban 2 argumentos".to_string());
        }

        // aca se harian los get de redis y nos devolveria un numero directamnte
        let na = get_cell_value(args[0], &cells);
        let nb = get_cell_value(args[1], &cells);

        return Ok(exec_command(cmd, na, nb));
    } else if args_str.contains(':') {
        let parts: Vec<&str> = args_str.split(':').map(|s| s.trim()).collect();
        if parts.len() != 2 {
            return Err("ERROR: rango inválido".to_string());
        }

        let celdas = expand_range(parts[0], parts[1]);
        let valores: Vec<i32> = celdas
            .into_iter()
            // aca se harian los get de redis y nos devolveria un numero directamnte
            .map(|nombre| get_cell_value(&nombre, &cells))
            .collect();

        return Ok(exec_command_range(cmd, valores));
    }

    Err("ERROR: argumentos inválidos".to_string())
}

fn expand_range(start: &str, end: &str) -> Vec<String> {
    let (start_col, start_row) = split_cell(start);
    let (end_col, end_row) = split_cell(end);

    let start_col_idx = col_str_to_index(&start_col);
    let end_col_idx = col_str_to_index(&end_col);

    let mut cells = Vec::new();
    for col_idx in start_col_idx..=end_col_idx {
        for row in start_row..=end_row {
            let col_str = index_to_col_str(col_idx);
            cells.push(format!("{col_str}{row}"));
        }
    }
    cells
}

fn get_cell_value(_cell: &str, cells: &[Vec<String>]) -> i32 {
    if let Ok((row, col)) = cell_to_index_zero_based(_cell) {
        if row < cells.len() && col < cells[row].len() {
            cells[row][col].parse::<i32>().unwrap_or_default()
        } else {
            0
        }
    } else {
        0
    }
}

fn col_str_to_index(col: &str) -> usize {
    col.chars().fold(0, |acc, c| {
        acc * 26 + ((c.to_ascii_uppercase() as u8 - b'A') as usize + 1)
    })
}

fn index_to_col_str(mut index: usize) -> String {
    let mut col = String::new();
    while index > 0 {
        index -= 1;
        col.insert(0, (b'A' + (index % 26) as u8) as char);
        index /= 26;
    }
    col
}

fn split_cell(cell: &str) -> (String, usize) {
    let (letters, numbers): (String, String) = cell.chars().partition(|c| c.is_alphabetic());
    (letters, numbers.parse().unwrap_or(1))
}

pub fn cell_to_index_zero_based(cell_reference: &str) -> Result<(usize, usize), String> {
    let (row, col) = cell_to_index(cell_reference)?;

    Ok(((row - 1) as usize, (col - 1) as usize))
}

fn cell_to_index(cell_reference: &str) -> Result<(u32, u32), String> {
    let mut col_part = String::new();
    let mut row_part = String::new();

    for c in cell_reference.chars() {
        if c.is_ascii_alphabetic() {
            col_part.push(c.to_ascii_uppercase());
        } else if c.is_ascii_digit() {
            row_part.push(c);
        } else {
            return Err(format!("Carácter inválido '{c}' en referencia de celda"));
        }
    }

    if col_part.is_empty() || row_part.is_empty() {
        return Err("Formato de celda incorrecto. Debe ser como 'A1' o 'AB23'".to_string());
    }

    let col_number = col_part.chars().rev().enumerate().fold(0, |acc, (i, c)| {
        let char_value = (c as u32) - ('A' as u32) + 1;
        acc + char_value * 26u32.pow(i as u32)
    });

    let row_number = row_part
        .parse::<u32>()
        .map_err(|_| "No se pudo parsear el número de fila".to_string())?;

    Ok((row_number, col_number))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zero_based() {
        assert_eq!(cell_to_index_zero_based("A1"), Ok((0, 0)));
        assert_eq!(cell_to_index_zero_based("B1"), Ok((0, 1)));
        assert_eq!(cell_to_index_zero_based("Z1"), Ok((0, 25)));
        assert_eq!(cell_to_index_zero_based("AA1"), Ok((0, 26)));
        assert_eq!(cell_to_index_zero_based("AB1"), Ok((0, 27)));
        assert_eq!(cell_to_index_zero_based("BA1"), Ok((0, 52)));
        assert_eq!(cell_to_index_zero_based("B2"), Ok((1, 1)));
        assert_eq!(cell_to_index_zero_based("XFD1048576"), Ok((1048575, 16383)));
    }
}
