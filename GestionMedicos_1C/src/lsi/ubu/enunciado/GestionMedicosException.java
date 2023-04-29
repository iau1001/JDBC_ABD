package lsi.ubu.enunciado;

import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GestionMedicosException:
 * Implementa las excepciones contextualizadas de la transacciones
 * de gestion de medicos
 * 
 * @author <a href="mailto:jmaudes@ubu.es">Jesus Maudes</a>
 * @author <a href="mailto:rmartico@ubu.es">Raul Marticorena</a>
 * @author <a href="mailto:pgdiaz@ubu.es">Pablo Garcia</a>
 * @version 1.0
 * @since 1.0 
 */
public class GestionMedicosException extends SQLException {

	private static final long serialVersionUID = 1L;
	
	public static final int CLIENTE_NO_EXISTE = 1;
	public static final int MEDICO_NO_EXISTE = 2;
	public static final int MEDICO_OCUPADO = 3;
	public static final int CONSULTA_NO_EXISTE = 4;
	public static final int CONSULTA_NO_ANULA = 5;

	private int codigo; // = -1;
	private String mensaje;

	private static Logger l = LoggerFactory.getLogger(GestionMedicosException.class);	

	public GestionMedicosException(int code) {
		codigo = code;
		String mensaje = null;

		switch (code) {
		case CLIENTE_NO_EXISTE:
			mensaje = "Cliente inexistente";
			break;
		case MEDICO_NO_EXISTE:
			mensaje = "Médico inexistente";
			break;
		case MEDICO_OCUPADO:
			mensaje = "Médico ocupado";
			break;
		case CONSULTA_NO_EXISTE:
			mensaje = "Consulta inexistente";
			break;
		case CONSULTA_NO_ANULA:
			mensaje = "La Consulta no se puede anular para la fecha introducida";
			break;
		}					

		l.error(mensaje);

		// Traza_de_pila
		for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
			l.info(ste.toString());
		}

	}

	@Override
	public String getMessage() { // Redefinicion del metodo de la clase
									// Exception
		return mensaje;
	}

	@Override
	public int getErrorCode() { // Redefinicion del metodo de la clase
								// SQLException
		return codigo;
	}

}
