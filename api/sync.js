// ./api/sync.js
import {
  fetchCSVFromS3,
  readProcessedList,
  saveProcessedList,
  testS3Connections,
  saveExecutionReport,
} from "../utils/s3Helpers.js";
import { sendToHubspot } from "../utils/hubspot.js";
import { ListObjectsV2Command } from "@aws-sdk/client-s3";

const AWS1_BUCKET = process.env.AWS1_BUCKET;
const s3Read = new (await import("@aws-sdk/client-s3")).S3Client({
  region: process.env.AWS1_REGION,
  credentials: {
    accessKeyId: process.env.AWS1_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS1_SECRET_ACCESS_KEY,
  },
});

export const config = {
  runtime: "nodejs",
};

export default async function handler(req, res) {
  console.log("🔌 Verificando conexión con buckets S3...");

  const s3Ok = await testS3Connections();
  if (!s3Ok) {
    return res.status(500).send("❌ Fallo en conexión a uno o ambos buckets S3.");
  }

  console.log("📃 Cargando historial...");
  const processed = await readProcessedList();

  const command = new ListObjectsV2Command({
    Bucket: AWS1_BUCKET,
    Prefix: "delta_negocio_",
  });

  const { Contents = [] } = await s3Read.send(command);

  const nuevosArchivos = Contents.map((obj) => obj.Key)
    .filter((key) => key.endsWith(".csv"))
    .filter((key) => !processed.includes(key));

  if (nuevosArchivos.length === 0) {
    console.log("🟡 No hay nuevos archivos para procesar.");
    return res.status(200).send("🟡 No hay archivos nuevos.");
  }

  for (const fileName of nuevosArchivos) {
    let resumen = "";
    try {
      console.log(`⬇️ Procesando archivo: ${fileName}`);
      const deals = await fetchCSVFromS3(fileName);

      if (!deals.length) {
        const mensaje = `⚠️ Archivo vacío: ${fileName}`;
        console.warn(mensaje);
        await saveExecutionReport(fileName, mensaje);
        continue;
      }

      console.log(`📨 Enviando ${deals.length} negocios a HubSpot...`);
      const resultado = await sendToHubspot(deals);

      resumen = `
📄 Procesado archivo: ${fileName}

📊 Total negocios en archivo: ${resultado.totalOriginal}
✅ Subidos exitosamente: ${resultado.totalProcesadosConExito}
❌ Fallidos en envío: ${resultado.totalFallidosEnEnvio}
🚫 Sin contacto válido: ${resultado.totalSinContacto}
🚫 Sin nombre: ${resultado.totalSinNombre}

📈 Tasa de éxito: ${resultado.tasaExito}%

🕒 Fecha de ejecución: ${new Date().toLocaleString("es-ES", { timeZone: "Europe/Madrid" })}
`;

      processed.push(fileName);
      console.log(`✅ Procesado exitosamente: ${fileName}`);
    } catch (error) {
      resumen = `❌ Error procesando ${fileName}:
${error.message}`;
      console.error(resumen);
    }

    await saveExecutionReport(fileName, resumen);
  }

  console.log("💾 Actualizando historial...");
  await saveProcessedList(processed);

  return res.status(200).send(`✅ Procesados ${nuevosArchivos.length} archivos nuevos.`);
}
