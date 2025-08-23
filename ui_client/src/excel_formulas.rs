pub fn suma(a: i32, b: i32) -> String {
    format!("{}", a + b)
}

pub fn suma_rango(nums: Vec<i32>) -> String {
    nums.iter().sum::<i32>().to_string()
}

pub fn resta(a: i32, b: i32) -> String {
    format!("{}", a - b)
}

pub fn resta_rango(nums: Vec<i32>) -> String {
    if nums.is_empty() {
        "0".to_string()
    } else {
        let result = nums.iter().skip(1).fold(nums[0], |acc, x| acc - x);
        result.to_string()
    }
}

pub fn multiplicacion(a: i32, b: i32) -> String {
    format!("{}", a * b)
}

pub fn multiplicacion_rango(nums: Vec<i32>) -> String {
    if nums.is_empty() {
        "0".to_string()
    } else {
        let result = nums.iter().product::<i32>();
        result.to_string()
    }
}

pub fn division(a: i32, b: i32) -> String {
    if b == 0 {
        "Error: Division by zero".to_string()
    } else {
        format!("{}", a / b)
    }
}

pub fn promedio(nums: Vec<i32>) -> String {
    if nums.is_empty() {
        "0".to_string()
    } else {
        let sum: i32 = suma_rango(nums.clone()).parse().unwrap_or(0);
        let count = nums.len() as i32;
        format!("{}", sum / count)
    }
}

pub fn modulo(a: i32, b: i32) -> String {
    format!("{}", a % b)
}

pub fn porcentaje(a: i32, b: i32) -> String {
    if b == 0 {
        return "Error: Division by zero".to_string();
    }
    let porcentaje = (a * 100) / b;
    format!("{porcentaje}%")
}

pub fn exec_command(cmd: &str, a: i32, b: i32) -> String {
    match cmd.to_uppercase().as_str() {
        "SUMA" => suma(a, b),
        "RESTA" => resta(a, b),
        "MUL" => multiplicacion(a, b),
        "DIV" => division(a, b),
        "MOD" => modulo(a, b),
        "PER" => porcentaje(a, b),
        _ => format!("Unknown command: {cmd}"),
    }
}

pub fn exec_command_range(cmd: &str, nums: Vec<i32>) -> String {
    match cmd.to_uppercase().as_str() {
        "SUMA" => suma_rango(nums),
        "RESTA" => resta_rango(nums),
        "MUL" => multiplicacion_rango(nums),
        "PROMEDIO" => promedio(nums),
        _ => format!("Unknown command: {cmd}"),
    }
}
