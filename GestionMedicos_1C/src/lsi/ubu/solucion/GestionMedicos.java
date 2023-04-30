package lsi.ubu.solucion;

import java.io.ByteArrayOutputStream;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsi.ubu.enunciado.GestionMedicosException;
import lsi.ubu.util.ExecuteScript;
import lsi.ubu.util.PoolDeConexiones;
import lsi.ubu.util.exceptions.SGBDError;
import lsi.ubu.util.exceptions.oracle.OracleSGBDErrorUtil;


/**
 * GestionMedicos:
 * Implementa la gestion de medicos segun el PDF de la carpeta enunciado
 * 
 * @author <a href="mailto:iau1001@alu.ubu.es">Irati Arraiza Urquiola</a>
 * @version 1.0
 * @since 1.0 
 */
public class GestionMedicos {
	
	private static Logger logger = LoggerFactory.getLogger(GestionMedicos.class);

	private static final String script_path = "sql/";

	public static void main(String[] args) throws SQLException{		
		tests();

		System.out.println("FIN.............");
	}
	
	public static void reservar_consulta(String m_NIF_cliente, 
			String m_NIF_medico,  Date m_Fecha_Consulta) throws SQLException {
		
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con=null;
		PreparedStatement st_select = null;
		PreparedStatement st_insert = null;
		PreparedStatement st_update = null;
		ResultSet rs = null;
		
		
		try{
			con = pool.getConnection();
			
			//Se obtiene el id del médico. Se lanza la excepción 'medico_no_existe' si no existe.
			st_select = con.prepareStatement("SELECT id_medico FROM MEDICO WHERE NIF=?");
			st_select.setString(1, m_NIF_medico);
			rs = st_select.executeQuery();
			if(!rs.next())
				throw new GestionMedicosException(GestionMedicosException.MEDICO_NO_EXISTE);
			int num_medico = rs.getInt(1);
			
			//Se inserta la nueva consulta.
			java.sql.Date m_sqlFecha= new java.sql.Date(m_Fecha_Consulta.getTime());
			st_insert = con.prepareStatement("INSERT INTO CONSULTA VALUES (seq_consulta.nextval,?,?,?)");
			st_insert.setDate(1, m_sqlFecha);
			st_insert.setInt(2, num_medico);
			st_insert.setString(3,m_NIF_cliente);
			st_insert.executeUpdate();
			
			//Se actualiza el num. de consultas del médico si no existe otra consulta no anulada en la misma fecha.
			//Si existe otra consulta no anulada en la misma fecha se lanza el error 'medico_ocupado'.
			st_update = con.prepareStatement("UPDATE MEDICO SET consultas=consultas+1 WHERE id_medico=?"+
					" and (SELECT COUNT(*) FROM CONSULTA join ANULACION ON consulta.id_consulta=anulacion.id_consulta"+
					" where fecha_consulta=? and id_medico=?)+1=(SELECT COUNT(*) FROM CONSULTA WHERE "+
					" fecha_consulta=? and id_medico=?)");
			st_update.setInt(1, num_medico);
			st_update.setDate(2, m_sqlFecha);
			st_update.setInt(3, num_medico);
			st_update.setDate(4, m_sqlFecha);
			st_update.setInt(5, num_medico);
			int n = st_update.executeUpdate();
			if (n==0) {
				throw new GestionMedicosException(GestionMedicosException.MEDICO_OCUPADO);
			}
			
			con.commit();
		} catch (SQLException e) {
			//Rollback con cualquier error.
			con.rollback();
			//Relanzar excepción.
			if (e instanceof GestionMedicosException)
				throw (GestionMedicosException)e;			
			//Si insertar la consulta levanta la excepción 'violación de fk' significa que el cliente no existe.
			//Se lanza el error 'cliente_no_existe'.
			if ( new OracleSGBDErrorUtil().checkExceptionToCode( e, SGBDError.FK_VIOLATED)) {
				throw new GestionMedicosException(GestionMedicosException.CLIENTE_NO_EXISTE);
			}
			//Si es cualquier otra excepción, se registra el mensaje y se lanza.
			logger.error(e.getMessage());
			throw e;
		} finally {
			//Se liberan los recursos.
			if(rs!=null) rs.close();
			if(st_select!=null) st_select.close();
			if(st_insert!=null) st_insert.close();
			if(st_update!=null) st_update.close();
			if(con!=null) con.close();
		}	
	}
	
	public static void anular_consulta(String m_NIF_cliente, String m_NIF_medico,  
			Date m_Fecha_Consulta, Date m_Fecha_Anulacion, String motivo)
			throws SQLException {
		
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con=null;
		PreparedStatement st_select_med = null;
		ResultSet rs_med = null;
		PreparedStatement st_select_cli = null;
		ResultSet rs_cli = null;
		PreparedStatement st_select_cons = null;
		ResultSet rs_cons = null;
		PreparedStatement st_insert = null;
		PreparedStatement st_update = null;

	
		try{
			con = pool.getConnection();
			
			//Se obtiene el id del médico. Se lanza la excepción 'medico_no_existe' si no existe.
			st_select_med = con.prepareStatement("SELECT id_medico FROM MEDICO WHERE NIF=?");
			st_select_med.setString(1, m_NIF_medico);
			rs_med = st_select_med.executeQuery();
			if(!rs_med.next())
				throw new GestionMedicosException(GestionMedicosException.MEDICO_NO_EXISTE);
			int num_medico = rs_med.getInt(1);
			
			//Se comprueba si existe el cliente. Si no existe se lanza el error 'cliente_no_existe'.
			st_select_cli = con.prepareStatement("SELECT NIF FROM CLIENTE WHERE NIF=?");
			st_select_cli.setString(1,m_NIF_cliente);
			rs_cli = st_select_cli.executeQuery();
			if(!rs_cli.next())
				throw new GestionMedicosException(GestionMedicosException.CLIENTE_NO_EXISTE);
			
			//Se obtiene el id de la consulta si existe y no está anulada.
			//Si está anulada o no existe la consulta, se lanza el error 'consulta_no_existe'.
			st_select_cons = con.prepareStatement("SELECT id_consulta FROM CONSULTA WHERE fecha_consulta=?"+
					" and NIF = ? and id_medico = ? and id_consulta not in (SELECT id_consulta from ANULACION)");
			st_select_cons.setDate(1, new java.sql.Date(m_Fecha_Consulta.getTime()));
			st_select_cons.setString(2, m_NIF_cliente);
			st_select_cons.setInt(3, num_medico);
			rs_cons = st_select_cons.executeQuery();
			if (!rs_cons.next()) {
				throw new GestionMedicosException(GestionMedicosException.CONSULTA_NO_EXISTE);
			}
			int num_consulta = rs_cons.getInt(1);
			
			//Se inserta la anulación.
			st_insert = con.prepareStatement("INSERT INTO ANULACION VALUES (seq_anulacion.nextval,?,?,?)");
			st_insert.setInt(1,num_consulta);
			st_insert.setDate(2, new java.sql.Date(m_Fecha_Anulacion.getTime()));
			st_insert.setString(3, motivo);
			st_insert.executeQuery();
			
			//Se actualiza el num. de consultas del médico si la fecha de anulación es como mínimo 2 días
			//anterior a la fecha de consulta.
			//Si la fecha de anulación no cumple ese mínimo, se lanza el error 'consulta_no_anula'.
			st_update = con.prepareStatement("UPDATE MEDICO SET consultas=consultas-1 WHERE id_medico=?"+
					" AND 2<=?");
			st_update.setInt(1, num_medico);
			st_update.setInt(2, Misc.howManyDaysBetween(m_Fecha_Consulta,m_Fecha_Anulacion));
			int n = st_update.executeUpdate();
			if (n==0) {
				throw new GestionMedicosException(GestionMedicosException.CONSULTA_NO_ANULA);
			}
			
			con.commit();
		} catch (SQLException e) {
			//Rollback con cualquier error.
			con.rollback();
			//Relanzar excepción.
			if (e instanceof GestionMedicosException) {
				throw (GestionMedicosException)e;
			}
			//Al insertar el registro de anulación, si el motivo es vacío se lanza la excepción 'motivo_vacio'.
			if ( new OracleSGBDErrorUtil().checkExceptionToCode( e, SGBDError.NOT_NULL_VIOLATED)) {
				throw new GestionMedicosException(GestionMedicosException.MOTIVO_VACIO);
			}
			//Si es cualquier otra excepción, se registra el mensaje y se lanza.
			logger.error(e.getMessage());
			throw e;
		} finally {
			//Se liberan los recursos.
			if (rs_med!=null) rs_med.close();
			if (st_select_med!=null) st_select_med.close();
			if (rs_cli!=null) rs_cli.close();
			if (st_select_cli!=null) st_select_cli.close();
			if (rs_cons!=null) rs_cons.close();
			if (st_select_cons!=null) st_select_cons.close();
			if (st_insert!=null) st_insert.close();
			if (st_update!=null) st_update.close();
			if (con!=null) con.close();
		}		
	}
	
	public static void consulta_medico(String m_NIF_medico)
			throws SQLException {

				
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con=null;
		PreparedStatement st_med = null;
		ResultSet rs_med = null;

	
		try{
			con = pool.getConnection();
			
			//Se obtiene el id del médico. Se lanza la excepción 'medico_no_existe' si no existe.
			st_med = con.prepareStatement("SELECT id_medico FROM MEDICO WHERE NIF=?");
			st_med.setString(1, m_NIF_medico);
			rs_med = st_med.executeQuery();
			if(!rs_med.next())
				throw new GestionMedicosException(GestionMedicosException.MEDICO_NO_EXISTE);
			int num_medico = rs_med.getInt(1);
			
		} catch (SQLException e) {
			//Rollback con cualquier error.
			con.rollback();
			//Relanzar excepción.
			if (e instanceof GestionMedicosException) {
				throw (GestionMedicosException)e;
			}
			//Si es cualquier otra excepción, se registra el mensaje y se lanza.
			logger.error(e.getMessage());
			throw e;
		} finally {
			//Se liberan los recursos.
			if (rs_med!=null) rs_med.close();
			if (st_med!=null) st_med.close();
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
		
		//Casos reservar consulta
		
		//Caso 1: El cliente no existe.
		try {
			//Reinicio filas
			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			reservar_consulta("11111111A","222222B",Misc.getCurrentDate());
			System.out.println("MAL: Cliente inexistente no levanta excepcion");
		} catch (SQLException e) {
			if (e.getErrorCode()==GestionMedicosException.CLIENTE_NO_EXISTE) {
				System.out.println("OK: Detecta cliente inexistente");
			} else {
				System.out.println("MAL: Cliente inexistente levanta excepcion "+e.getMessage());
			}
		} finally {
			if (cll_reinicia!=null) cll_reinicia.close();
			if (conn!=null) conn.close();
		}
		
		//Caso 2: El médico no existe.
		try {
			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			reservar_consulta("12345678A","111111B",Misc.getCurrentDate());
			System.out.println("MAL: Medico inexistente no levanta excepcion");
		} catch (SQLException e) {
			if (e.getErrorCode()==GestionMedicosException.MEDICO_NO_EXISTE) {
				System.out.println("OK: Detecta medico inexistente");
			} else {
				System.out.println("MAL: Medico inexistente levanta excepcion "+e.getMessage());
			}
		} finally {
			if (cll_reinicia!=null) cll_reinicia.close();
			if (conn!=null) conn.close();
		}
		
		//Caso 3: Reserva para médico que tiene una consulta en esa fecha. El médico está ocupado.
		SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy");
		java.util.Date fecha = null;
		try {
			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			fecha = format.parse("25-03-2022");
			reservar_consulta("12345678A","8766788Y",fecha);
			System.out.println("MAL: Medico ocupado no levanta excepcion");
		} catch (SQLException e) {
			if (e.getErrorCode()==GestionMedicosException.MEDICO_OCUPADO){
				System.out.println("OK: Detecta medico ocupado");
			} else {
				System.out.println("MAL: Medico ocupado levanta excepcion "+e.getMessage());
			}
		} catch (ParseException e) {
			logger.error("Error en el test al parsear la fecha desde cadena.");
		} finally {
			if (cll_reinicia!=null) cll_reinicia.close();
			if (conn!=null) conn.close();
		}
		
		//Caso 4: Todo OK. Reserva para una fecha libre y cliente y médico correctos.
		Statement st = null;
		ResultSet rs = null;
		fecha = null;
		try {
			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			fecha = format.parse("20-03-2022");
			reservar_consulta("12345678A","8766788Y",fecha);
			st = conn.createStatement();
			rs = st.executeQuery("SELECT id_consulta||fecha_consulta||consulta.NIF||consulta.id_medico"+
			"||medico.NIF||nombre||ape1||ape2||especialidad||consultas"+
					" from consulta join medico on consulta.id_medico=medico.id_medico"+
			" order by consulta.id_consulta");
			String resultado = "";
			while (rs.next()) {
				resultado+=rs.getString(1);
			}
			String esperado = "124/03/2312345678A1222222BJoseSanchezSabchezMedicina General0"+
					"225/03/2287654321B28766788YAlejandraAmosGarciaOncologia2"+
					"320/03/2212345678A28766788YAlejandraAmosGarciaOncologia2";
			if (resultado.equals(esperado)) {
				System.out.println("OK: Insercion correcta, las consultas y el numero de consultas coinciden");
			}else {
				System.out.println("MAL: Insercion incorrecta, las consultas y el numero de consultas no coinciden");
				System.out.println("Se obtiene...*" + resultado + "*");
				System.out.println("Y deberia ser*" + esperado + "*");
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} catch (ParseException e) {
			logger.error("Error en el test al parsear la fecha desde cadena.");
		} finally {
			if (rs!=null) rs.close();
			if (st!=null) st.close();
			if (cll_reinicia!=null) cll_reinicia.close();
			if (conn!=null) conn.close();
		}
		
		//Casos anular consulta
		
		//Caso 1: El cliente no existe.
		java.util.Date fecha_consulta = null;
		java.util.Date fecha_anulacion = null;
		try {
			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			fecha_consulta = format.parse("25-03-2022");
			fecha_anulacion = format.parse("10-03-2022");
			anular_consulta("87654322B","8766788Y",fecha_consulta,fecha_anulacion,"Viaje");
			System.out.println("MAL: Cliente inexistente no levanta excepcion");
		} catch (SQLException e) {
			if (e.getErrorCode()==GestionMedicosException.CLIENTE_NO_EXISTE){
				System.out.println("OK: Detecta cliente inexistente");
			} else {
				System.out.println("MAL: Cliente inexistente levanta excepcion "+e.getMessage());
			}
		} catch (ParseException e) {
			logger.error("Error en el test al parsear la fecha desde cadena.");
		} finally {
			if (cll_reinicia!=null) cll_reinicia.close();
			if (conn!=null) conn.close();
		}
		
		//Caso 2: El médico no existe.
		fecha_consulta = null;
		fecha_anulacion = null;
		try {
			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			fecha_consulta = format.parse("25-03-2022");
			fecha_anulacion = format.parse("10-03-2022");
			anular_consulta("87654321B","8766777Y",fecha_consulta,fecha_anulacion,"Viaje");
			System.out.println("MAL: Medico inexistente no levanta excepcion");
		} catch (SQLException e) {
			if (e.getErrorCode()==GestionMedicosException.MEDICO_NO_EXISTE){
				System.out.println("OK: Detecta medico inexistente");
			} else {
				System.out.println("MAL: Medico inexistente levanta excepcion "+e.getMessage());
			}
		} catch (ParseException e) {
			logger.error("Error en el test al parsear la fecha desde cadena.");
		} finally {
			if (cll_reinicia!=null) cll_reinicia.close();
			if (conn!=null) conn.close();
		}
		
		//Caso 3: La fecha de la consulta no es correcta. No existe la consulta.
		fecha_consulta = null;
		fecha_anulacion = null;
		try {
			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			fecha_consulta = format.parse("24-03-2023");
			fecha_anulacion = format.parse("10-03-2022");
			anular_consulta("87654321B","8766788Y",fecha_consulta,fecha_anulacion,"Viaje");
			System.out.println("MAL: Consulta inexistente no levanta excepcion");
		} catch (SQLException e) {
			if (e.getErrorCode()==GestionMedicosException.CONSULTA_NO_EXISTE){
				System.out.println("OK: Detecta consulta inexistente");
			} else {
				System.out.println("MAL: Consulta inexistente levanta excepcion "+e.getMessage());
			}
		} catch (ParseException e) {
			logger.error("Error en el test al parsear la fecha desde cadena.");
		} finally {
			if (cll_reinicia!=null) cll_reinicia.close();
			if (conn!=null) conn.close();
		}
		
		//Caso 4: La consulta ya ha sido anulada anteriormente, por lo que no existe.
		//Debe lanzar el error 'consulta_no_existe'.
		fecha_consulta = null;
		fecha_anulacion = null;
		try {
			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			fecha_consulta = format.parse("24-03-2023");
			fecha_anulacion = format.parse("10-03-2023");
			anular_consulta("12345678A","222222B",fecha_consulta,fecha_anulacion,"Viaje");
			System.out.println("MAL: Consulta inexistente no levanta excepcion");
		} catch (SQLException e) {
			if (e.getErrorCode()==GestionMedicosException.CONSULTA_NO_EXISTE){
				System.out.println("OK: Detecta consulta inexistente");
			} else {
				System.out.println("MAL: Consulta inexistente levanta excepcion "+e.getMessage());
			}
		} catch (ParseException e) {
			logger.error("Error en el test al parsear la fecha desde cadena.");
		} finally {
			if (cll_reinicia!=null) cll_reinicia.close();
			if (conn!=null) conn.close();
		}
		
		//Caso 5: Se intenta anular una consulta con un día de antelación.
		//Debe lanzar el error 'consulta_no_anula'.
		fecha_consulta = null;
		fecha_anulacion = null;
		try {
			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			fecha_consulta = format.parse("25-03-2022");
			fecha_anulacion = format.parse("24-03-2022");
			anular_consulta("87654321B","8766788Y",fecha_consulta,fecha_anulacion,"Viaje");
			System.out.println("MAL: Consulta no anulable no levanta excepcion");
		} catch (SQLException e) {
			if (e.getErrorCode()==GestionMedicosException.CONSULTA_NO_ANULA){
				System.out.println("OK: Detecta consulta no anulable");
			} else {
				System.out.println("MAL: Consulta no anulable levanta excepcion "+e.getMessage());
			}
		} catch (ParseException e) {
			logger.error("Error en el test al parsear la fecha desde cadena.");
		} finally {
			if (cll_reinicia!=null) cll_reinicia.close();
			if (conn!=null) conn.close();
		}
		
		//Caso 6: Se intenta anular una consulta sin motivo.
		//Debe lanzar el error 'motivo_vacio'.
		fecha_consulta = null;
		fecha_anulacion = null;
		try {
			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			fecha_consulta = format.parse("25-03-2022");
			fecha_anulacion = format.parse("22-03-2022");
			anular_consulta("87654321B","8766788Y",fecha_consulta,fecha_anulacion,"");
			System.out.println("MAL: Motivo inexistente no levanta excepcion");
		} catch (SQLException e) {
			if (e.getErrorCode()==GestionMedicosException.MOTIVO_VACIO){
				System.out.println("OK: Detecta motivo inexistente");
			} else {
				logger.error(e.getMessage());
				System.out.println("MAL: Motivo inexistente levanta excepcion "+e.getMessage());
			}
		} catch (ParseException e) {
			logger.error("Error en el test al parsear la fecha desde cadena.");
		} finally {
			if (cll_reinicia!=null) cll_reinicia.close();
			if (conn!=null) conn.close();
		}
		
		//Caso 7: Todo OK. Anula una consulta no anulada anteriormente con una fecha de anulación válida.
		fecha_consulta = null;
		fecha_anulacion = null;
		try {
			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			fecha_consulta = format.parse("25-03-2022");
			fecha_anulacion = format.parse("22-03-2022");
			anular_consulta("87654321B","8766788Y",fecha_consulta,fecha_anulacion,"Viaje");
			st = conn.createStatement();
			rs = st.executeQuery("SELECT id_anulacion||anulacion.id_consulta||fecha_anulacion"+
			"||motivo_anulacion||medico.id_medico||medico.NIF||nombre||ape1||ape2||especialidad||consultas"+
			" from anulacion join consulta on anulacion.id_consulta=consulta.id_consulta"+
			" join medico on consulta.id_medico=medico.id_medico"+
			" order by anulacion.id_anulacion");
			String resultado = "";
			while (rs.next()) {
				resultado+=rs.getString(1);
			}
			String esperado = "1124/02/23Enfermedad infecciosa1222222BJoseSanchezSabchezMedicina General0"+
					"2222/03/22Viaje28766788YAlejandraAmosGarciaOncologia0";
			if (resultado.equals(esperado)) {
				System.out.println("OK: Insercion correcta,las consultas anuladas y el numero de consultas coinciden.");
			}else {
				System.out.println("MAL: Insercion incorrecta, las consultas anuladas y el numero de consultas no coinciden");
				System.out.println("Se obtiene...*" + resultado + "*");
				System.out.println("Y deberia ser*" + esperado + "*");
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} catch (ParseException e) {
			logger.error("Error en el test al parsear la fecha desde cadena.");
		} finally {
			if (rs!=null) rs.close();
			if (st!=null) st.close();
			if (cll_reinicia!=null) cll_reinicia.close();
			if (conn!=null) conn.close();
		}
		
		//Casos consulta_medico
		
		//Caso 1: El médico no existe.
		try {
			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			consulta_medico("12345678A");
			System.out.println("MAL: Medico inexistente no levanta excepcion");
			} catch (SQLException e) {
			if (e.getErrorCode()==GestionMedicosException.MEDICO_NO_EXISTE){
				System.out.println("OK: Detecta medico inexistente");
			} else {
				System.out.println("MAL: Medico inexistente levanta excepcion "+e.getMessage());
			}
		} finally {
			if (cll_reinicia!=null) cll_reinicia.close();
			if (conn!=null) conn.close();
		}
		
		//Caso 2: Todo OK. Se muestran las consultas del médico 2. 
		try {
			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			ByteArrayOutputStream b = new ByteArrayOutputStream();
			PrintStream ps = new PrintStream(b);
			System.setOut(ps); 
			consulta_medico("8766788Y");
			String resultado = b.toString().trim();
			String esperado = "IDCONSULTA"+"\t"+"FECHA"+"\t\t"+"IDMEDICO"+"\t"+"NIFCLIENTE"+"\t"+"ANULADA"+"\n"+
					   "2\t\t2022-03-25\t2\t\t87654321B\tNo";
			System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
			if (resultado.equals(esperado)) {
				System.out.println("OK: Los datos mostrados son correctos. Muestra bien las consultas.");
			}else {
				System.out.println("MAL: Los datos mostrados son incorrectos. No muestra bien las consultas.");
				System.out.println("Se obtiene...*" + resultado + "*");
				System.out.println("Y deberia ser*" + esperado + "*");
			}
		} catch (SQLException e) {
			System.out.println("MAL: Ha surgido un error.");
			logger.error(e.getMessage());
		}finally {
			if (cll_reinicia!=null) cll_reinicia.close();
			if (conn!=null) conn.close();
		}
		
		//Caso 3: Todo OK. Se muestran las consultas del médico 1.
		try {
			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			ByteArrayOutputStream b = new ByteArrayOutputStream();
			PrintStream ps = new PrintStream(b);
			System.setOut(ps);
			consulta_medico("222222B");
			String resultado = b.toString().trim();
			String esperado = "IDCONSULTA"+"\t"+"FECHA"+"\t\t"+"IDMEDICO"+"\t"+"NIFCLIENTE"+"\t"+"ANULADA"+"\n"+
					   "1\t\t2023-03-24\t1\t\t12345678A\tSí";
			System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
			if (resultado.equals(esperado)) {
				System.out.println("OK: Los datos mostrados son correctos. Muestra bien las consultas.");
			}else {
				System.out.println("MAL: Los datos mostrados son incorrectos. No muestra bien las consultas.");
				System.out.println("Se obtiene...*" + resultado + "*");
				System.out.println("Y deberia ser*" + esperado + "*");
			}
		} catch (SQLException e) {
			System.out.println("MAL: Ha surgido un error.");
			logger.error(e.getMessage());
		}finally {
			if (cll_reinicia!=null) cll_reinicia.close();
			if (conn!=null) conn.close();
		}
		
		//Caso final, se prueban los tres métodos.
		//Todo OK. Inserciones y modificaciones correctas.
		//Se reserva una consulta anulada anteriormente al médico 1
		//y se reserva y se anula una consulta en una fecha válida al médico 2.
		//Por último, se llama al método consulta_medico para ver las consultas que tiene cada médico.
		Statement st_cons = null;
		ResultSet rs_cons = null;
		Statement st_anul = null;
		ResultSet rs_anul = null;
		java.util.Date fecha_reserva1 = null;
		java.util.Date fecha_reserva2 = null;
		java.util.Date fecha_anul= null;
		try {
			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			fecha_reserva1 = format.parse("24-03-2023");
			fecha_reserva2 = format.parse("28-04-2022");
			fecha_anul = format.parse("26-04-2022");
			reservar_consulta("12345678A","222222B",fecha_reserva1);
			reservar_consulta("78677433R","8766788Y",fecha_reserva2);
			anular_consulta("78677433R","8766788Y",fecha_reserva2,fecha_anul,"Viaje");
			st_cons = conn.createStatement();
			rs_cons = st_cons.executeQuery("SELECT id_consulta||fecha_consulta||consulta.NIF||consulta.id_medico"+
			"||medico.id_medico||medico.NIF||nombre||ape1||ape2||especialidad||consultas"+
			" from consulta join medico on consulta.id_medico=medico.id_medico"+
			" order by consulta.id_consulta");
			String resultado = "";
			while (rs_cons.next()) {
				resultado+=rs_cons.getString(1);
			}
			st_anul = conn.createStatement();
			rs_anul = st_anul.executeQuery("SELECT id_anulacion||anulacion.id_consulta||fecha_anulacion"+
			"||motivo_anulacion||consulta.id_consulta||fecha_consulta||NIF||id_medico"+
			" from anulacion join consulta on anulacion.id_consulta=consulta.id_consulta"+
			" order by anulacion.id_anulacion");
			while (rs_anul.next()) {
				resultado+=rs_anul.getString(1);
			}
			String esperado = "124/03/2312345678A11222222BJoseSanchezSabchezMedicina General1"+
					          "225/03/2287654321B228766788YAlejandraAmosGarciaOncologia1"+
					          "324/03/2312345678A11222222BJoseSanchezSabchezMedicina General1"+
					          "428/04/2278677433R228766788YAlejandraAmosGarciaOncologia1"+
					          "1124/02/23Enfermedad infecciosa124/03/2312345678A1"+
					          "2426/04/22Viaje428/04/2278677433R2";
			if (resultado.equals(esperado)) {
				System.out.println("OK: Las inserciones y actualizaciones se realizan correctamente");
			}else {
				System.out.println("MAL: Las inserciones y actualizaciones no se realizan correctamente");
				System.out.println("Se obtiene...*" + resultado + "*");
				System.out.println("Y deberia ser*" + esperado + "*");
			}
			
			ByteArrayOutputStream b = new ByteArrayOutputStream();
			PrintStream ps = new PrintStream(b);
			System.setOut(ps);
			consulta_medico("222222B");
			consulta_medico("8766788Y");
			resultado = b.toString().trim();
			esperado = "IDCONSULTA"+"\t"+"FECHA"+"\t\t"+"IDMEDICO"+"\t"+"NIFCLIENTE"+"\t"+"ANULADA"+"\n"+
					   "3\t\t2023-03-24\t1\t\t12345678A\tNo\n1\t\t2023-03-24\t1\t\t12345678A\tSí\n"+
					   "IDCONSULTA"+"\t"+"FECHA"+"\t\t"+"IDMEDICO"+"\t"+"NIFCLIENTE"+"\t"+"ANULADA"+"\n"+
					   "2\t\t2022-03-25\t2\t\t87654321B\tNo\n4\t\t2022-04-28\t2\t\t78677433R\tSí";
			System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
			if (resultado.equals(esperado)) {
				System.out.println("OK: Los datos mostrados son correctos. Muestra bien las consultas.");
			}else {
				System.out.println("MAL: Los datos mostrados son incorrectos. No muestra bien las consultas.");
				System.out.println("Se obtiene...*" + resultado + "*");
				System.out.println("Y deberia ser*" + esperado + "*");
			}
		} catch (SQLException e) {
			System.out.println("MAL: Ha surgido un error.");
			logger.error(e.getMessage());
		} catch (ParseException e) {
			logger.error("Error en el test al parsear la fecha desde cadena.");
		} finally {
			if (rs_anul!=null) rs_anul.close();
			if (st_anul!=null) st_anul.close();
			if (rs_cons!=null) rs_cons.close();
			if (st_cons!=null) st_cons.close();
			if (cll_reinicia!=null) cll_reinicia.close();
			if (conn!=null) conn.close();
		}
	}
}