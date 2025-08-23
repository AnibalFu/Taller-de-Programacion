mod constantes;
mod entrada_salida;
mod estructuras;
mod operaciones;
mod tests;

pub fn interpretar_texto(text: Vec<String>) -> (String, String) {
    entrada_salida::lectura::interpretar_archivo(text, &mut false)
}
