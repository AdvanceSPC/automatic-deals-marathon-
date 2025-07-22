// ./utils/hubspot.js
import fetch from "node-fetch";

export async function sendToHubspot(deals) {
  const apiKey = process.env.HUBSPOT_API_KEY;
  const url = `https://api.hubapi.com/crm/v3/objects/deals/batch/create`;
  const batchSize = 100;

  let totalProcessed = 0;
  let totalSkipped = 0;

  for (let i = 0; i < deals.length; i += batchSize) {
    const batch = deals.slice(i, i + batchSize);
    
    console.log(`🔍 Validando contactos del batch ${Math.floor(i/batchSize) + 1}...`);
    
    // Validar que todos los contactos existan antes de enviar el batch
    const validDeals = [];
    const invalidContactIds = [];
    
    for (const deal of batch) {
      const contactId = deal.associations[0].to.id;
      const contactExists = await checkContactExists(contactId);
      
      if (contactExists) {
        validDeals.push(deal);
      } else {
        invalidContactIds.push({
          contactId: contactId,
          dealName: deal.properties.dealname || 'Sin nombre'
        });
      }
    }
    
    // Registrar negocios que no se subirán
    if (invalidContactIds.length > 0) {
      console.warn(`❌ ${invalidContactIds.length} negocio(s) NO se subirán por contactos inexistentes:`);
      invalidContactIds.forEach(item => {
        console.warn(`   • Negocio: "${item.dealName}" - Contacto inexistente: ${item.contactId}`);
      });
      totalSkipped += invalidContactIds.length;
    }
    
    // Solo enviar si hay negocios válidos
    if (validDeals.length === 0) {
      console.log(`⚠️ Batch ${Math.floor(i/batchSize) + 1}: Todos los contactos son inválidos, saltando batch completo`);
      continue;
    }
    
    // Enviar solo los negocios válidos
    const res = await fetch(url, {
      method: "POST",
      headers: { 
        "Content-Type": "application/json",
        "Authorization": `Bearer ${apiKey}`
      },
      body: JSON.stringify({ inputs: validDeals }),
    });

    if (!res.ok) {
      console.error(`❌ Error en batch ${Math.floor(i/batchSize) + 1}:`, await res.text());
    } else {
      totalProcessed += validDeals.length;
      console.log(`✅ Batch ${Math.floor(i/batchSize) + 1} enviado exitosamente: ${validDeals.length}/${batch.length} negocios`);
    }
  }
  
  console.log(`📊 Resumen final: ${totalProcessed} negocios subidos ✅ | ${totalSkipped} negocios omitidos ❌`);
}

// Función para verificar si existe un contacto en HubSpot
async function checkContactExists(contactId) {
  const apiKey = process.env.HUBSPOT_API_KEY;
  const url = `https://api.hubapi.com/crm/v3/objects/contacts/${contactId}`;
  
  try {
    const res = await fetch(url, {
      method: "GET",
      headers: { 
        "Authorization": `Bearer ${apiKey}`
      }
    });
    
    return res.ok;
  } catch (error) {
    console.error(`❌ Error verificando contacto ${contactId}:`, error);
    return false;
  }
}