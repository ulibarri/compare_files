const fs = require("fs");
const csv = require("csv-parser");
const fastcsv = require("fast-csv");

const filtersFile = "20250916_FILTERS.csv";
const specialtyFile = "20250916_SPECIALTY.csv";
const roughDraftFile = "20250916_ROUGH_DRAFT.csv";

// Función para leer un CSV y devolver su contenido en un arreglo de objetos
function readCSV(filename) {
  return new Promise((resolve, reject) => {
    const results = [];
    fs.createReadStream(filename)
      .pipe(csv())
      .on("data", (data) => results.push(data))
      .on("end", () => resolve(results))
      .on("error", reject);
  });
}

async function processCSNs() {
  try {
    const filters = await readCSV(filtersFile);
    const specialty = await readCSV(specialtyFile);
    const roughDraft = await readCSV(roughDraftFile);

    const filtersCSNs = new Set(filters.map((row) => row["CSN"]));
    const specialtyCSNs = new Set(specialty.map((row) => row["CSN"]));

    // 1. CSN en FILTERS pero no en SPECIALTY
    const onlyInFilters = [...filtersCSNs].filter((csn) => !specialtyCSNs.has(csn));
    const resultFiltersNotSpecialty = roughDraft.filter((row) =>
      onlyInFilters.includes(row["CSN"])
    ).map((row) => ({
      "PACE MRN": row["PACE MRN"],
      Patient: row["Patient"],
      "Patient Address": row["Patient Address"],
      Time: row["Time"],
      Token: row["Provider/Resource"]
    }));

    // Guardar archivo
    fastcsv
      .write(resultFiltersNotSpecialty, { headers: true })
      .pipe(fs.createWriteStream("trips_In_Filters_Not_In_Specialty.csv"));

    // 2. CSN en SPECIALTY pero no en FILTERS
    const onlyInSpecialty = [...specialtyCSNs].filter((csn) => !filtersCSNs.has(csn));
    const resultSpecialtyNotFilters = roughDraft.filter((row) =>
      onlyInSpecialty.includes(row["CSN"])
    ).map((row) => ({
      "PACE MRN": row["PACE MRN"],
      Patient: row["Patient"],
      "Patient Address": row["Patient Address"],
      Time: row["Time"],
      Token: row["Provider/Resource"]
    }));

    // Guardar archivo
    fastcsv
      .write(resultSpecialtyNotFilters, { headers: true })
      .pipe(fs.createWriteStream("trips_In_Specialty_Not_In_Filters.csv"));

    console.log("✅ Archivos generados exitosamente");
  } catch (err) {
    console.error("❌ Error procesando archivos:", err);
  }
}

processCSNs();
