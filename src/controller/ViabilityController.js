const connection = require('../database/connection');

module.exports = {
  async index(req, res) {
    try {
      const { inscricaoImobiliaria, cnaes } = req.body;

      if (!inscricaoImobiliaria || inscricaoImobiliaria === '' || !cnaes) {
        return res.status(400).json({ erro: 'Inscrição inválida!' });
      }

      const inscricaoFormated = inscricaoImobiliaria.replace(/([^0-9])/g, '');

      if (!await validaInscrica(inscricaoFormated)) {
        return res.status(400).json({ erro: 'Inscrição inválida!' });
      }

      const leiMae = await getLeiMae(inscricaoFormated);

      if (leiMae === '') {
        return res.status(400).json({ erro: 'Inscrição não Geocodificada!' });
      }

      const fl_response = await getEdificada(inscricaoFormated);

      const cnaesResult = [];

      for (cnaeCod of cnaes) {
        const cnaeFormated = cnaeCod.replace(/([^0-9])/g, '');

        let numTent = 0;
        let response = await getArrayUsos(cnaeFormated, leiMae, numTent);  
        numTent++;

        while (!response && numTent < 5) {
          response = await getArrayUsos(cnaeFormated, leiMae, numTent);
          numTent++;
        }

        if (!response) {
          cnaesResult.push({
            cnae: cnaeCod,
            erro: 'Código de Uso Inválido.'
          });
          continue;
        }
        
        const array_parecer = await getParecer(inscricaoFormated, response.cd_classe, response.cd_secao);

        if (array_parecer.length > 0) {
          for (let i = 0; i < array_parecer.length; i++) {
            var parecer = '';

            //CONCATENANDO PARECER COM LIMITACOES DE USO
            if (parecer === '') { //CASO NAO TENHA CAIDO EM NENHUM CASO ESPECIAL(excecao)
              parecer = array_parecer[i]['parecer'] + '. ';
              if (array_parecer[i]['comp_adeq_uso'] !== '') {
                parecer += array_parecer[i]['comp_adeq_uso'] + ' = ';
              }
              if (array_parecer[i]['limitacao']!== '') {
                parecer += array_parecer[i]['limitacao'];
              }
              if (array_parecer[i]['tp_uso'] !== '') {
                if (parecer !== '') {
                  parecer += ' ' + array_parecer[i]['tp_uso'] + ' = ' + array_parecer[i]['uso'] + '.';
                } else {
                  parecer += array_parecer[i]['tp_uso'] + ' = ' + array_parecer[i]['uso'] + '.';
                }
              }
            }

            cnaesResult.push({
              cnae: cnaeCod,
              // status: array_parecer[i].status,
              lei: array_parecer[i].lei_2,
              lei_alteracao: array_parecer[i].lei,
              nm_zon: array_parecer[i].nm_zon,
              de_zon: array_parecer[i].zon,
              parecer,
              porcentagem: array_parecer[i].porcentagem,
              viavel: (array_parecer[i].letra_parecer === 'A'),
            });
          }
        }
      }

      return res.json({ 
        inscricaoImobiliaria: inscricaoImobiliaria,
        fl_edificada: fl_response.fl_edificada,
        fl_edificada_desc : fl_response.fl_edificada_desc,
        cnaes: cnaesResult,
      });
    } catch (err){
      return res.status(500).json({ erro: 'Internal server error.' });
    }
  },
};

async function getEdificada(inscricaoImobiliaria) {
  try {
    var response = {};

    const { rows } = await connection.raw(`
      select nu_insc_imbl, tp_ocpc_lote, 
      (select descricao from item_bci ib where ib.opcao_bci::varchar=ci.tp_ocpc_lote and ib.item_bci =26) as descricao
      from ctu.cotr_imobiliario ci where ci.nu_insc_imbl = '${inscricaoImobiliaria}'
    `);

    if (rows && rows.length > 0) {
      response.fl_edificada = rows[0].descricao === 'Construído' ? 'S' : 'N';
      response.fl_edificada_desc = rows[0].descricao;
    } else {
      response.fl_edificada = 'Não encontrada';
      response.fl_edificada_desc = 'Não encontrada';
    }

    return response;
  } catch (error) {
    throw new Error('Internal server error.');
  }
}


async function validaInscrica(inscricaoImobiliaria) {
  try {
    const { rows } = await connection.raw(`
      SELECT nu_insc_imbl, nu_pess, in_canc_cimb, cd_lote
      FROM ctu.cotr_imobiliario 
      WHERE nu_insc_imbl = '${inscricaoImobiliaria}'
    `);

    if (!rows ||rows.length <= 0) return false;

    var cotrImobiliario = rows[0];

    if (cotrImobiliario.in_canc_cimb === '*') return false;

    var cd_lote = cotrImobiliario.cd_lote || inscricaoImobiliaria.substring(0, 11);

    const { rows: cadLote } = await connection.raw(`
      SELECT mslink FROM cad_lote WHERE cd_lote = '${cd_lote}'
    `);

    if (!cadLote ||cadLote.length <= 0) return false;

    return true;
  } catch (error) {
    throw new Error('Internal server error.');
  }
}

async function getLeiMae(inscricaoImobiliaria) {
  try {
    const { rows: search } = await connection.raw(`
      SELECT DISTINCT public.plan_zon_pd_pri_pdp.lei_2
      FROM ((ctu.cotr_imobiliario RIGHT JOIN public.cad_lote
      ON ctu.cotr_imobiliario.cd_lote = public.cad_lote.cd_lote)
      RIGHT JOIN public.plan_zon_pd_pri_pdp_join
      ON public.cad_lote.mslink = public.plan_zon_pd_pri_pdp_join.cd_mslink_lote)
      RIGHT JOIN public.plan_zon_pd_pri_pdp
      ON public.plan_zon_pd_pri_pdp_join.cd_mslink_zon = public.plan_zon_pd_pri_pdp.mslink
      WHERE ctu.cotr_imobiliario.nu_insc_imbl = '${inscricaoImobiliaria}'
    `);

    let leiInsc = '';

    if (search && search.length > 0) leiInsc = search[0].lei_2;

    if (leiInsc === '') {
      const { rows: search1 } = await connection.raw(`
        select pz.lei_2
        from public.plan_zon_pd_pri_pdp pz , public.cad_lote cl
        where st_intersects(pz.geom, cl.geom)
        and cl.cd_lote = '${inscricaoImobiliaria.substring(0, 11)}'
      `);

      if (search1 && search1.length > 0) leiInsc = search1[0].lei_2;
    }

    // Caso nao retorne nada eh pq area do lote estah completamente em cima de sv
    if (leiInsc === '') {
      const { rows: search2 } = await connection.raw(`
        SELECT DISTINCT public.plan_zon_pd_sv.lei_2
        FROM ((ctu.cotr_imobiliario RIGHT JOIN public.cad_lote
        ON ctu.cotr_imobiliario.cd_lote = public.cad_lote.cd_lote)
        RIGHT JOIN public.plan_sv_join
        ON public.cad_lote.mslink = public.plan_sv_join.cd_mslink_lote)
        RIGHT JOIN public.plan_zon_pd_sv
        ON public.plan_sv_join.cd_mslink_sv = public.plan_zon_pd_sv.mslink
        WHERE ctu.cotr_imobiliario.nu_insc_imbl = '${inscricaoImobiliaria}'
      `);

      if (search2 && search2.length > 0) leiInsc = search2[0].lei_2;
    }

    // Caso nao retorne nada eh pq area do lote esta completamente em cima de projeto de engenharia
    if (leiInsc === '') {
      const { rows: search3 } = await connection.raw(`
        SELECT DISTINCT public.plan_zon_pd_sv_proj.lei_2
        FROM ((ctu.cotr_imobiliario RIGHT JOIN public.cad_lote
        ON ctu.cotr_imobiliario.cd_lote = public.cad_lote.cd_lote)
        RIGHT JOIN public.plan_sv_proj_join
        ON public.cad_lote.mslink = public.plan_sv_proj_join.cd_mslink_lote)
        RIGHT JOIN public.plan_zon_pd_sv_proj
        ON public.plan_sv_proj_join.cd_mslink_sv = public.plan_zon_pd_sv_proj.mslink
        WHERE ctu.cotr_imobiliario.nu_insc_imbl = '${inscricaoImobiliaria}'
      `);

      if (search3 && search3.length > 0) leiInsc = search3[0].lei_2;
    }

    // Se as três retornarem vazio é pq nao existe na tabela join
    return leiInsc;

  } catch (error) {
    throw new Error('Internal server error.');
  }
}

async function getArrayUsos(codUso, leiMae, numTent) {
  try {
    switch (numTent) {
      case 1:
        codUso = codUso.substring(0, 5);
        break;
      case 2:
        codUso = codUso.substring(0, 4);
        break;
      case 3:
        codUso = codUso.substring(0, 3);
        break;
      case 4:
        codUso = codUso.substring(0, 2);
        break;

      default:
        break;
    }

    const { rows: search } = await connection.raw(`
      SELECT cd_plan_zon_uso, lei, cd_secao, desc_uso, cd_classe
      FROM viabilidade.plan_zon_uso
      WHERE regexp_replace(cd_classe,'[^0-9]','','g') = regexp_replace('${codUso}','[^0-9]','','g')
      AND lei = '${leiMae}'
    `);

    if (!search || search.length <= 0) return false;

    return search[0];
  } catch (error) {
    throw new Error('Internal server error.');
  }
}

async function getParecer(inscricaoImobiliaria, uso, solicita) {
  var array_ret = [];
  var cont_tmp = 0;

  var array_val = await getZonValida(inscricaoImobiliaria);

  const { rows: search } = await connection.raw(`
    SELECT DISTINCT
      public.plan_zon_pd_pri_pdp.tp_zon,
      public.plan_zon_pd_pri_pdp.nm_zon,
      (st_area(st_intersection(public.cad_lote.geom, public.plan_zon_pd_pri_pdp.geom))/st_area(public.cad_lote.geom)) * 100 as porcentagem,
      regexp_replace(public.plan_zon_pd_pri_pdp.tp_zon,'[^a-zA-Z]','','g') as letras,
      public.plan_zon_pd_pri_pdp.lei,
      public.plan_zon_pd_pri_pdp.lei_2	
    FROM 
      ((ctu.cotr_imobiliario RIGHT JOIN public.cad_lote
      ON ctu.cotr_imobiliario.cd_lote = public.cad_lote.cd_lote) 
      RIGHT JOIN public.plan_zon_pd_pri_pdp_join
      ON public.cad_lote.mslink = public.plan_zon_pd_pri_pdp_join.cd_mslink_lote)
      RIGHT JOIN public.plan_zon_pd_pri_pdp 
      ON public.plan_zon_pd_pri_pdp_join.cd_mslink_zon = public.plan_zon_pd_pri_pdp.mslink
    WHERE 
      ctu.cotr_imobiliario.nu_insc_imbl='${inscricaoImobiliaria}'
  `);

  if (search.length > 0) {
    for(row of search) {
      var ret = '';
      var ret_uso = '';
      var campo = `uso_${row.letras.toLocaleLowerCase()}`;
      var tp_zo = row.tp_zon;
      var porcentagem = row.porcentagem;
      
      //split no campo $campo para pegar as tres primeiras letras do zoneamento
      var ini = campo.split('_');
      var iniciais = ini[1].toUpperCase();
      var num = search.length;

      if (num === 1) { //SE O LOTE FOR SÓ DE UM TIPO DE ZONEAMENTO
        if (row.letras === '') {
          
        } else {

          const { rows: row1 } = await connection.raw(`
            SELECT DISTINCT		
              viabilidade.plan_zon_uso.${campo},
              public.plan_zon_pd_pri_pdp.tp_zon,
              public.plan_zon_pd_pri_pdp.lei_2,
              '${uso}' as uso,
              (select desc_uso from viabilidade.plan_zon_uso where cd_classe='${uso}') as desc_uso,
              viabilidade.plan_zon_areas.nm_zon,
              public.plan_zon_pd_pri_pdp.lei
            FROM 
              ((((ctu.cotr_imobiliario LEFT JOIN public.cad_lote
              ON ctu.cotr_imobiliario.cd_lote = public.cad_lote.cd_lote) 
              LEFT JOIN public.plan_zon_pd_pri_pdp_join
              ON public.cad_lote.mslink = public.plan_zon_pd_pri_pdp_join.cd_mslink_lote)
              LEFT JOIN public.plan_zon_pd_pri_pdp 
              ON public.plan_zon_pd_pri_pdp_join.cd_mslink_zon = public.plan_zon_pd_pri_pdp.mslink)
              LEFT JOIN viabilidade.plan_zon_uso
              ON  public.plan_zon_pd_pri_pdp.lei_2=viabilidade.plan_zon_uso.lei)  
              LEFT JOIN viabilidade.plan_zon_areas
              ON (public.plan_zon_pd_pri_pdp.lei=viabilidade.plan_zon_areas.lei
              AND public.plan_zon_pd_pri_pdp.tp_zon=viabilidade.plan_zon_areas.tp_zon)
            WHERE 
              ctu.cotr_imobiliario.nu_insc_imbl='${inscricaoImobiliaria}'
              AND viabilidade.plan_zon_uso.cd_classe='${uso}'
              AND public.plan_zon_pd_pri_pdp.tp_zon='${tp_zo}'
          `);
  
          //1º split retorna a letra do tipo de adequacao na primeira parte do split, ou seja, 
          //no $parecer_array[0](ex: A) e as demais parte(s) ($parecer_array[1],etc...) as limitacoes e usos (ex: 10-p)
          var parecer_array = row1[0][campo].split('-');				
          var parecer_texto = parecer_array[0];

          const { rows: row2 } = await connection.raw(`
            SELECT viabilidade.plan_zon_adeq.desc_adeq
            FROM viabilidade.plan_zon_adeq
            WHERE viabilidade.plan_zon_adeq.tp_adeq='${parecer_texto}'
          `);
          
          //retorna a adequecao das areas (ex: Tolerável)	
          var adequacao = row2[0].desc_adeq;
          
          var limitacao = parecer_array;
          var complemento_adeq = '';
          var nums = '';

          for (let i = 0; i < limitacao.length; i++) {  //percorre o array de retorno do 2º split
            
            if (i !== 0) { //ignora a posicao 0 pois é o parecer
              if (i > 1) {
                complemento_adeq += `/${limitacao[i]}`;
              } else {
                complemento_adeq += limitacao[i];
              }
              if (limitacao[i] !== '') { //se o array nao for vazio busca limitacoes e/ou usos
                var aux = limitacao[i];
                
                if (!isNaN(aux) || (aux === '*')) { //se é nro => consulta na tabela plan_zon_adeq_le
                  if (nums === '') {
                      nums += aux;
                  } else {
                      nums += `,${aux}`;
                  }
                  const { rows: search3 } = await connection.raw(`
                    SELECT viabilidade.plan_zon_adeq_le.desc_le, viabilidade.plan_zon_adeq_le.tp_le
                    FROM viabilidade.plan_zon_adeq_le
                    WHERE viabilidade.plan_zon_adeq_le.tp_le='${aux}'AND lei='${row1[0].lei_2}'
                  `);
                        
                  var row4 = search3[0];

                  if (ret !== '') {
                    ret += `. ${row4.tp_le} - ${row4.desc_le}`; 
                  } else {
                    ret += `${row4.tp_le} - ${row4.desc_le}`;
                  }

                } else { //senao é nro=> consulta na tabela plan_zon_adequacao_uso
                  const { rows: search4 } = await connection.raw(`
                    SELECT viabilidade.plan_zon_adequacao_uso.desc_adeq_uso,
                    viabilidade.plan_zon_adequacao_uso.tp_adeq_uso
                    FROM viabilidade.plan_zon_adequacao_uso
                    WHERE viabilidade.plan_zon_adequacao_uso.tp_adeq_uso='${aux}'
                  `);

                  var row5 = search4[0];
                  ret_uso += row5.desc_adeq_uso;
                }
              }
            }
          }

          var tu = '';
          if (ret_uso !== ''){
            tu = row5.tp_adeq_uso;
          }

          array_ret[cont_tmp] = {
            parecer: adequacao,
            porcentagem,
            limitacao: ret,
            uso: ret_uso,
            tp_uso: tu,
            nm_zon: row1[0].tp_zon,
            zon: row1[0].nm_zon,
            comp_adeq_uso: complemento_adeq,
            lei_2: row1[0].lei_2,
            lei: row1[0].lei,
            letra_parecer: parecer_texto,
            numeros_parecer: nums,
            cd_sv: array_val[0].cd_sv,
            marcacao: '1',
            status: '1',
          };
          cont_tmp++;
        }
        
      } else { //SE O LOTE FOR DE MAIS DE UM TIPO DE ZONEAMENTO FAZ OS CÁLCULOS PARA O PARECER FINAL	
        //split no campo $campo para pegar as tres primeiras letras do zoneamento
        var ini = campo.split('_');
        var iniciais = ini[1].toUpperCase();

        //retorna os usos de adequação da tabela plan_zon_uso  (ex: A-10-p)
        const { rows: search5 } = await connection.raw(`
          SELECT DISTINCT --a unica alteração foi colocar o distinct 			
            viabilidade.plan_zon_uso.${campo},
            public.plan_zon_pd_pri_pdp.tp_zon,
            public.plan_zon_pd_pri_pdp.lei_2,
            '${uso}' as uso,
            (select desc_uso from viabilidade.plan_zon_uso where cd_classe='${uso}') as desc_uso,
            viabilidade.plan_zon_areas.nm_zon,
            public.plan_zon_pd_pri_pdp.lei
          FROM 
            ((((ctu.cotr_imobiliario LEFT JOIN public.cad_lote
            ON ctu.cotr_imobiliario.cd_lote = public.cad_lote.cd_lote) 
            LEFT JOIN public.plan_zon_pd_pri_pdp_join
            ON public.cad_lote.mslink = public.plan_zon_pd_pri_pdp_join.cd_mslink_lote)
            LEFT JOIN public.plan_zon_pd_pri_pdp 
            ON public.plan_zon_pd_pri_pdp_join.cd_mslink_zon = public.plan_zon_pd_pri_pdp.mslink)
            LEFT JOIN viabilidade.plan_zon_uso
            ON  public.plan_zon_pd_pri_pdp.lei_2=viabilidade.plan_zon_uso.lei)  
            LEFT JOIN viabilidade.plan_zon_areas
            ON (public.plan_zon_pd_pri_pdp.lei=viabilidade.plan_zon_areas.lei
            AND public.plan_zon_pd_pri_pdp.tp_zon=viabilidade.plan_zon_areas.tp_zon)
          WHERE 
            ctu.cotr_imobiliario.nu_insc_imbl='${inscricaoImobiliaria}'
            --AND viabilidade.plan_zon_uso.cd_secao='${solicita}' 
            AND viabilidade.plan_zon_uso.cd_classe='${uso}'
            AND public.plan_zon_pd_pri_pdp.tp_zon='${tp_zo}'
        `);

        var compara = '';
        if (array_val[0].status === '1') {//é pq é para marcar algum
          compara = array_val[0].tp_zon;
        } //senao volta todos, ou seja, nao marca nada
        
        for (row11 of search5) {
          var ret = '';
          var ret_uso = '';
          
          if (row11[campo] === '0' || row11[campo] === '' || row11[campo] === null) {
            array_ret[cont_tmp] = {
              parecer: 'Proibido o que requer quanto o Zoneamento',
              porcentagem,
              limitacao: '',
              uso: 'Proibido o que requer quanto o Zoneamento',
              tp_uso: '',
              nm_zon: row11.tp_zon,
              zon: row11.nm_zon,
              comp_adeq_uso: '',
              lei_2: row11.lei_2,
              lei: row11.lei,
              letra_parecer: 'P',
              numeros_parecer: '',
              cd_sv: array_val[0].cd_sv,
              marcacao: '',
              status: '1',
            };
            cont_tmp++;
          } else {
              //1º split retorna a letra do tipo de adequacao na primeira parte do split, ou seja, 
              //no $parecer_array[0](ex: A) e as demais parte(s) ($parecer_array[1],etc...) as limitacoes e usos (ex: 10-p)
              var parecer_array = row11[campo].split('-');		
              var parecer_texto = parecer_array[0];

              const { rows: search6 } = await connection.raw(`
                SELECT viabilidade.plan_zon_adeq.desc_adeq
                FROM viabilidade.plan_zon_adeq
                WHERE viabilidade.plan_zon_adeq.tp_adeq='${parecer_texto}'
              `);

              var row_ = search6[0];
              var adequacao = row_.desc_adeq;
              
              var limitacao = parecer_array;
              var complemento_adeq = '';
              var nums = '';

              for (let i = 0; i < limitacao.length; i++) {  //percorre o array de retorno do 2º split
                if (i !== 0){ //ignora a posicao 0 pois é o parecer
                  if (i > 1) {
                    complemento_adeq += `/${limitacao[i]}`;
                  } else {
                    complemento_adeq += limitacao[i];
                  }
                  if (limitacao[i] !== '') { //se o array nao for vazio busca limitacoes e/ou usos
                    var aux = limitacao[i];
                  
                    if (!isNaN(aux) || (aux === '*')) { //se é nro => consulta na tabela plan_zon_adeq_le

                      if (nums === '') {
                        nums += aux;
                      } else {
                        nums += `,${aux}`;
                      }

                      const { rows: sql_busca4 } = await connection.raw(`
                        SELECT viabilidade.plan_zon_adeq_le.desc_le, viabilidade.plan_zon_adeq_le.tp_le
                        FROM viabilidade.plan_zon_adeq_le
                        WHERE viabilidade.plan_zon_adeq_le.tp_le='${aux}'AND lei='${row11.lei_2}'
                      `);
                            
                      var row4 = sql_busca4[0];

                      if (ret !== '') {
                        ret += `. ${row4.tp_le} - ${row4.desc_le}`; 
                      } else {
                        ret += `${row4.tp_le} - ${row4.desc_le}`;
                      }
                    } else { //senao é nro=> consulta na tabela plan_zon_adequacao_uso
                      const { rows: search4 } = await connection.raw(`
                        SELECT viabilidade.plan_zon_adequacao_uso.desc_adeq_uso,
                        viabilidade.plan_zon_adequacao_uso.tp_adeq_uso
                        FROM viabilidade.plan_zon_adequacao_uso
                        WHERE viabilidade.plan_zon_adequacao_uso.tp_adeq_uso='${aux}'
                      `);

                      var row5 = search4[0];
                      ret_uso += row5.desc_adeq_uso;
                    }
                  }
                }
              }

              var tu = '';
              if (ret_uso !== ''){
                tu = row5.tp_adeq_uso;
              }

              array_ret[cont_tmp] = {
                parecer: adequacao,
                porcentagem,
                limitacao: ret,
                uso: ret_uso,
                tp_uso: tu,
                nm_zon: row11.tp_zon,
                zon: row11.nm_zon,
                comp_adeq_uso: complemento_adeq,
                lei_2: row11.lei_2,
                lei: row11.lei,
                letra_parecer: parecer_texto,
                numeros_parecer: nums,
                cd_sv: '',
                marcacao: '',
                status: array_val[0].status,
              };
              cont_tmp++;
            
          }
          
        } //fim do while( $row1 = $bd2->getNextRow()
      }
    } 
    
  }
  return array_ret;
}


async function getZonValida(inscricaoImobiliaria) {
  var cd_lote = inscricaoImobiliaria.substring(0, 11);

  var array_sv = [];
  var array_sv_aux = [];
  let count_aux = 0;

  try {
    const { rows: search } = await connection.raw(`
      SELECT * FROM (
      SELECT DISTINCT ON (e.cd_logr,tp_zon)
            e.cd_logr,e.cd_zon
            ,trim(regexp_replace(e.tp_zon,' ','','g')) as tp_zon
            ,e.lei,e.lei_2
            ,CASE WHEN st_touches(f.geom,e.intersecao) THEN 'true'
            WHEN st_intersects(f.geom,e.intersecao) THEN 'true'
            WHEN st_distance(f.geom,e.intersecao)<=5 THEN 'true'
            ELSE 'false'
            END as resposta
            ,f.cd_sv_order as prioridade
            ,f.cd_sv
      FROM (
        SELECT 
          c.cd_logr,c.cd_sv,c.geom_centerline,c.geom_lote,c.cd_lote,d.mslink as mslink_zon,d.cd_zon,d.tp_zon,d.nm_zon,d.lei,d.lei_2,d.geom as geom_zon,d.geom2 as geom2_zon,ST_Distance(ST_Intersection(c.geom_lote,d.geom),c.geom_centerline) as distancia,st_area(ST_Intersection(c.geom_lote,d.geom)) as area,ST_Intersection(c.geom_lote,d.geom) as intersecao
        FROM (
          SELECT 
              b.cd_logr,
              b.cd_sv,b.geom as geom_centerline,a.geom as geom_lote,a.cd_lote, st_area(ST_Intersection(a.geom,b.geom)) as area_inter_lote_centerline,st_astext(st_centroid(b.geom)),st_distance(b.geom,a.geom) as dist
            FROM cad_lote a,cad_centerline b
            WHERE 
            st_isvalid(a.geom) = true 
            AND st_isvalid(b.geom) = true
            AND ST_Distance(a.geom,b.geom) < 30
            AND a.cd_lote = '${cd_lote}'
            AND (
              trim(b.cd_logr) IN (select distinct trim(ctu.cotr_imobiliario.cd_logr) FROM ctu.cotr_imobiliario WHERE ctu.cotr_imobiliario.nu_insc_imbl='${inscricaoImobiliaria}')
              OR trim(b.cd_logr) IN (select distinct trim(ctu.cotr_imobiliario.cd_logr_tes2) FROM ctu.cotr_imobiliario WHERE ctu.cotr_imobiliario.nu_insc_imbl='${inscricaoImobiliaria}')
              OR trim(b.cd_logr) IN (select distinct trim(ctu.cotr_imobiliario.cd_logr_tes3) FROM ctu.cotr_imobiliario WHERE ctu.cotr_imobiliario.nu_insc_imbl='${inscricaoImobiliaria}')
              OR trim(b.cd_logr) IN (select distinct trim(ctu.cotr_imobiliario.cd_logr_tes4) FROM ctu.cotr_imobiliario WHERE ctu.cotr_imobiliario.nu_insc_imbl='${inscricaoImobiliaria}')
            )
        ) as c, plan_zon_pd_pri_pdp d 
        WHERE 
        st_isvalid(c.geom_lote) = true 
        AND st_isvalid(d.geom) = true
        AND st_intersects(c.geom_lote,d.geom)
        AND c.geom_lote && d.geom
        AND st_area(ST_Intersection(c.geom_lote,d.geom)) >10
        ORDER BY distancia
      )as e, plan_zon_pd_sv f
      WHERE
      st_intersects(st_line_interpolate_point(e.geom_centerline,0.50),f.geom)
      and
      (st_intersects(e.geom_lote,f.geom) OR st_touches(e.geom_lote,f.geom))
      AND
      CASE WHEN st_touches(f.geom,e.intersecao) THEN 'true'
        WHEN st_intersects(f.geom,e.intersecao) THEN 'true'
        WHEN st_distance(f.geom,e.intersecao)<=5 THEN 'true'
        ELSE 'false'
        END = 'true'
            ) as k
      ORDER BY prioridade,cd_logr
    `);

    if (search.length > 0) {

      for(value of search) {
        array_sv_aux[count_aux] = {
          tp_zon: value.tp_zon,
          lei: value.lei,
          lei_2: value.lei_2,
          cd_logr: value.cd_logr,
          prioridade: value.prioridade,
          cd_sv: value.cd_sv,
        };
        count_aux++;
      }
      
			if (array_sv_aux.length !== 0) {//SE EXITIR RETORNO
			
        if (array_sv_aux.length > 1) {//SE FOR MAIS DO QUE UM RETORNO ANALISAR
					if (array_sv_aux[0].prioridade === array_sv_aux[1].prioridade) {//SE EXISTIR MAIS DE UM COM A MESMA PRIORIDADE
						//funcao que verifica se tem mais de um logr. 
						//Paassando por parametro o prioridade para poder ignorar o resto dos que estao no array
						var varios_logrs = verificaLogr(array_sv_aux, array_sv_aux[0].prioridade);
						
						if (varios_logrs) {//verificar qual esta no STM e tratar
							array_sv = verificaLogrStm(inscricaoImobiliaria, cd_lote);			
						} else {
              //voltar todos eles
              array_sv[0] = { status: '2' };
						}
          } else {//SENAO VOLTA O DE MAIOR PRIORIDADE
            array_sv[0] = {
              status: '1',
              tp_zon: array_sv_aux[0].tp_zon,
              lei: array_sv_aux[0].lei,
              lei_2: array_sv_aux[0].lei_2,
              cd_sv: array_sv_aux[0].cd_sv,
            };
					}			
        } else {//SENAO JÁ VOLTA ESSE RESULTADO
          array_sv[0] = { 
            status: '1',
            tp_zon: array_sv_aux[0].tp_zon,
            lei: search[0].lei,
            lei_2: search[0].lei_2,
            cd_sv: array_sv_aux[0].cd_sv,
          };
				}
			}else{
				//se remeter ao estudo
				array_sv[0] = { status: '2' };
			}		
		}else{
			//se remeter a estudo
			array_sv[0] = { status: '2' };
		}

    return array_sv;
  } catch (error) {
    console.log(error);
  }
}

function verificaLogr(array_sv_aux, prioridade){
  let varios_logrs = false;
  let logr = '';

  for (let i = 0; i < array_sv_aux.length; i++) {
    if(array_sv_aux[i].prioridade == prioridade){				
      if ((logr != '') && (logr != array_sv_aux[i].cd_logr)) {
        varios_logrs = true;
      } else {
        varios_logrs = false;
      }			
      logr = array_sv_aux[i].cd_logr;
    }
  }

  return varios_logrs;
}

async function verificaLogrStm(inscricao, cd_lote){
  var array_ret = [];

  const { rows: search } = await connection.raw(`
    SELECT * FROM (
      SELECT DISTINCT ON (e.cd_logr,tp_zon)
        e.cd_logr,e.cd_zon
        --,e.tp_zon
        ,trim(regexp_replace(e.tp_zon,' ','','g')) as tp_zon
        ,e.lei,e.lei_2
          ,
          CASE WHEN st_touches(f.geom,e.intersecao) THEN 'true'
          WHEN st_intersects(f.geom,e.intersecao) THEN 'true'
          WHEN st_distance(f.geom,e.intersecao)<=5 THEN 'true'
          ELSE 'false'
          END as resposta
          ,f.cd_sv_order as prioridade
          ,f.cd_sv
      FROM (
        --lote com zoneamento
        SELECT 
          c.cd_logr,c.cd_sv,c.geom_centerline,c.geom_lote,c.cd_lote,d.mslink as mslink_zon,d.cd_zon,d.tp_zon,d.nm_zon,d.lei,d.lei_2,d.geom as geom_zon,d.geom2 as geom2_zon,ST_Distance(ST_Intersection(c.geom_lote,d.geom),c.geom_centerline) as distancia,st_area(ST_Intersection(c.geom_lote,d.geom)) as area,ST_Intersection(c.geom_lote,d.geom) as intersecao
          
        FROM (
          --cruzamento todas as centerline do lote (PEGA SEMPRE O PRIMEIRO QUE É O MAIS PROXIMO)
            SELECT --DISTINCT ON (b.cd_logr)
              b.cd_logr,
              b.cd_sv,b.geom as geom_centerline,a.geom as geom_lote,a.cd_lote, st_area(ST_Intersection(a.geom,b.geom)) as area_inter_lote_centerline,st_astext(st_centroid(b.geom)),st_distance(b.geom,a.geom) as dist
            FROM cad_lote a,cad_centerline b
            WHERE 
            st_isvalid(a.geom) = true 
            AND st_isvalid(b.geom) = true
            AND a.cd_lote = '${cd_lote}'			
            AND (
              trim(b.cd_logr) IN (select distinct trim(ctu.cotr_imobiliario.cd_logr) FROM ctu.cotr_imobiliario WHERE ctu.cotr_imobiliario.nu_insc_imbl='${inscricao}')
              OR trim(b.cd_logr) IN (select distinct trim(ctu.cotr_imobiliario.cd_logr_tes2) FROM ctu.cotr_imobiliario WHERE ctu.cotr_imobiliario.nu_insc_imbl='${inscricao}')
              OR trim(b.cd_logr) IN (select distinct trim(ctu.cotr_imobiliario.cd_logr_tes3) FROM ctu.cotr_imobiliario WHERE ctu.cotr_imobiliario.nu_insc_imbl='${inscricao}')
              OR trim(b.cd_logr) IN (select distinct trim(ctu.cotr_imobiliario.cd_logr_tes4) FROM ctu.cotr_imobiliario WHERE ctu.cotr_imobiliario.nu_insc_imbl='${inscricao}')
            )
        ) as c, plan_zon_pd_pri_pdp d 
        WHERE 
        st_isvalid(c.geom_lote) = true 
        AND st_isvalid(d.geom) = true
        AND st_intersects(c.geom_lote,d.geom)
        AND c.geom_lote && d.geom
        AND st_area(ST_Intersection(c.geom_lote,d.geom)) >10 -- desconsidera as areas muito miudinhas mtas vezes que só tocam
        ORDER BY distancia
      )as e, plan_zon_pd_sv f
      WHERE
      st_intersects(st_line_interpolate_point(e.geom_centerline,0.50),f.geom)
      and
      (st_intersects(e.geom_lote,f.geom) OR st_touches(e.geom_lote,f.geom))
      AND
      CASE WHEN st_touches(f.geom,e.intersecao) THEN 'true'
        WHEN st_intersects(f.geom,e.intersecao) THEN 'true'
        --WHEN st_distance(f.geom,e.intersecao)<=5 THEN 'true'
        ELSE 'false'
        END = 'true'
      ) as k 
      WHERE 
      --prioridade = 1 AND 
      trim(k.cd_logr) = (select distinct trim(ctu.cotr_imobiliario.cd_logr) FROM ctu.cotr_imobiliario WHERE ctu.cotr_imobiliario.nu_insc_imbl='${inscricao}' )
      ORDER BY cd_logr
  `);
  
  if ( search.length > 0) {
    for(value of search) {
      if (search.length > 1) {
        array_ret[0] = { status: '2' };
      } else {
        array_ret[0] = { 
          status: '1',
          tp_zon: value.tp_zon,
          lei: value.lei,
          lei_2: value.lei_2,
          cd_sv: value.cd_sv,
        };
      }
    }
  } else {
    array_ret[0] = { status: '2' };
  }

  return array_ret;
}