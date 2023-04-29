package lsi.ubu.enunciado;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsi.ubu.util.ExecuteScript;
import lsi.ubu.util.PoolDeConexiones;


/**
 * GestionMedicos:
 * Implementa la gestion de medicos segun el PDF de la carpeta enunciado
 * 
 * @author <a href="mailto:jmaudes@ubu.es">Jesus Maudes</a>
 * @author <a href="mailto:rmartico@ubu.es">Raul Marticorena</a>
 * @author <a href="mailto:pgdiaz@ubu.es">Pablo Garcia</a>
 * @version 1.0
 * @since 1.0 
 */
public class EsqueletoGestionMedicos {
	
	private static Logger logger = LoggerFactory.getLogger(EsqueletoGestionMedicos.class);

	private static final String script_path = "sql/";

	public static void main(String[] args) throws SQLException{		
		tests();

		System.out.println("FIN.............");
	}
	
	public static void reservar_consulta(String m_NIF_cliente, 
			String m_NIF_medico,  Date m_Fecha_Consulta) throws SQLException {
		
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con=null;

	
		try{
			con = pool.getConnection();
			
		} catch (SQLException e) {
			//Completar por el alumno			
			
			logger.error(e.getMessage());
			throw e;		

		} finally {
			/*A rellenar por el alumno, liberar recursos*/
		}
		
		
	}
	
	public static void anular_consulta(String m_NIF_cliente, String m_NIF_medico,  
			Date m_Fecha_Consulta, Date m_Fecha_Anulacion, String motivo)
			throws SQLException {
		
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con=null;

	
		try{
			con = pool.getConnection();
			
		} catch (SQLException e) {
			//Completar por el alumno			
			
			logger.error(e.getMessage());
			throw e;		

		} finally {
			/*A rellenar por el alumno, liberar recursos*/
		}		
	}
	
	public static void consulta_medico(String m_NIF_medico)
			throws SQLException {

				
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con=null;

	
		try{
			con = pool.getConnection();
			
		} catch (SQLException e) {
			//Completar por el alumno			
			
			logger.error(e.getMessage());
			throw e;		

		} finally {
			/*A rellenar por el alumno, liberar recursos*/
		}		
	}
	
	static public void creaTablas() {
		ExecuteScript.run(script_path + "gestion_medicos.sql");
	}

	static void tests() throws SQLException{
		creaTablas();
		
		PoolDeConexiones pool = PoolDeConexiones.getInstance();		
		
		//Relatar caso por caso utilizando el siguiente procedure para inicializar los datos
		
		CallableStatement cll_reinicia=null;
		Connection conn = null;
		
		try {
			//Reinicio filas
			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
		} catch (SQLException e) {				
			logger.error(e.getMessage());			
		} finally {
			if (cll_reinicia!=null) cll_reinicia.close();
			if (conn!=null) conn.close();
		
		}			
		
	}
}
