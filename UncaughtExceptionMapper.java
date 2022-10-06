package ariba.openapi.invoicing.exception.mappers;

import ariba.restapi.core.util.RestAPILog;
import com.sap.ariba.procurement.common.core.ErrorResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

/**
 *  Response when some unhandled exception is thrown
 */
public class UncaughtExceptionMapper implements ExceptionMapper<Throwable>
{

    public Response toResponse (Throwable ex)
    {
        RestAPILog.error(ex,"%s: exception");
        ErrorResponse response = new ErrorResponse("Internal Server Error",
                                                   Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
                                                   ex.getMessage());
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).
            entity(response).type(MediaType.APPLICATION_JSON).build();

    }

}

