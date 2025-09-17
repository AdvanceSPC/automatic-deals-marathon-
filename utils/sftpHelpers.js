// utils/sftpHelpers.js
import { Client } from 'ssh2-sftp-client';

const SFTP_CONFIG = {
  host: process.env.SFTP_HOST,
  port: parseInt(process.env.SFTP_PORT) || 22,
  username: process.env.SFTP_USER,
  password: process.env.SFTP_PASS,
  readyTimeout: 30000,
  retries: 3,
  retry_factor: 2
};

export async function testSFTPConnection() {
  const sftp = new Client();
  try {
    console.log(`🔌 Conectando a SFTP: ${SFTP_CONFIG.host}:${SFTP_CONFIG.port}`);
    await sftp.connect(SFTP_CONFIG);
    
    console.log("✅ Conexión SFTP establecida");
    
    // Verificar que podemos listar archivos
    const files = await sftp.list('/');
    console.log(`📂 Directorio raíz accesible, ${files.length} elementos encontrados`);
    
    return true;
  } catch (error) {
    console.error("❌ Error conectando a SFTP:", error.message);
    return false;
  } finally {
    await sftp.end();
  }
}

export async function listCSVFiles() {
  const sftp = new Client();
  try {
    await sftp.connect(SFTP_CONFIG);
    console.log("📂 Listando archivos CSV del servidor SFTP...");
    
    const files = await sftp.list('/');
    const csvFiles = files
      .filter(file => file.type === '-' && file.name.endsWith('.csv'))
      .filter(file => file.name.startsWith('delta_negocio_'))
      .map(file => ({
        name: file.name,
        size: file.size,
        modifyTime: file.modifyTime
      }))
      .sort((a, b) => a.name.localeCompare(b.name));

    console.log(`📋 Archivos CSV encontrados: ${csvFiles.length}`);
    csvFiles.slice(0, 5).forEach((file, index) => {
      console.log(`   ${index + 1}. ${file.name} (${Math.round(file.size / 1024)}KB)`);
    });
    
    if (csvFiles.length > 5) {
      console.log(`   ... y ${csvFiles.length - 5} archivos más`);
    }

    return csvFiles.map(f => f.name);
  } catch (error) {
    console.error("❌ Error listando archivos SFTP:", error.message);
    throw error;
  } finally {
    await sftp.end();
  }
}

export async function fetchCSVFromSFTP(fileName) {
  const sftp = new Client();
  try {
    console.log(`📥 Descargando ${fileName} desde SFTP...`);
    await sftp.connect(SFTP_CONFIG);
    
    // Obtener info del archivo
    const stat = await sftp.stat(`/${fileName}`);
    console.log(`📊 Tamaño del archivo: ${Math.round(stat.size / 1024)}KB`);
    
    // Descargar como buffer
    const buffer = await sftp.get(`/${fileName}`);
    const csvContent = buffer.toString('utf8');
    
    console.log(`✅ Archivo descargado exitosamente (${csvContent.length} caracteres)`);
    
    // Parsear CSV manualmente usando el mismo separador ";" que usabas antes
    console.log("🔄 Parseando CSV...");
    const lines = csvContent.split('\n').filter(line => line.trim());
    
    if (lines.length === 0) {
      console.warn("⚠️ Archivo CSV vacío");
      return [];
    }

    // Obtener headers de la primera línea
    const headers = lines[0].split(';').map(header => header.trim());
    console.log(`📋 Headers encontrados (${headers.length}): ${headers.slice(0, 5).join(', ')}${headers.length > 5 ? '...' : ''}`);
    
    const deals = [];
    let invalidCount = 0;

    // Procesar cada línea de datos (empezando desde la línea 1)
    for (let i = 1; i < lines.length; i++) {
      const values = lines[i].split(';').map(value => value.trim());
      
      // Crear objeto row mapeando headers con values
      const row = {};
      headers.forEach((header, index) => {
        row[header] = values[index] || '';
      });

      // Validar que existe contact_id (igual que en tu código original)
      if (!row.contact_id) {
        invalidCount++;
        if (invalidCount <= 3) {
          console.warn(`⚠️ Fila ${i + 1}: Sin contact_id válido`);
        }
        continue;
      }

      // Crear deal con EXACTAMENTE la misma estructura que tenías antes
      const deal = {
        properties: {
          dealname: row.linea || null,
          concepto: row.concepto || null,
          region: row.region || null,
          microsite_calculado: row.microsite_calculado || null,
          provincia_homologada: row.provincia_homologada || null,
          ciudad_centro: row.ciudad_centro || null,
          centro: row.centro || null,
          closedate: row.closedate || null,
          grupo: row.grupo || null,
          marca: row.marca || null,
          equipo: row.equipo || null,
          genero_edad: row.genero_edad || null,
          agrupador_categoria: row.agrupador_categoria || null,
          actividad: row.actividad || null,
          talla__codigo_: row.talla__codigo_ || null,
          nombre_campana: row.nombre_campana || null,
          amount: row.amount || null,
          dealstage: row.dealstage || null,
          pipeline: row.pipeline || null,
        },
        associations: [
          {
            types: [
              {
                associationCategory: "HUBSPOT_DEFINED",
                associationTypeId: 3,
              },
            ],
            to: {
              id: row.contact_id,
              type: "contact",
            },
          },
        ],
      };

      deals.push(deal);
    }

    if (invalidCount > 3) {
      console.warn(`⚠️ ... y ${invalidCount - 3} registros inválidos adicionales`);
    }

    console.log(`✅ Deals válidos transformados: ${deals.length}`);
    if (deals.length !== (lines.length - 1)) {
      console.warn(`⚠️ Se filtraron ${(lines.length - 1) - deals.length} registros inválidos`);
    }
    
    return deals;
    
  } catch (error) {
    console.error(`❌ Error descargando ${fileName}:`, error.message);
    throw error;
  } finally {
    await sftp.end();
  }
}
