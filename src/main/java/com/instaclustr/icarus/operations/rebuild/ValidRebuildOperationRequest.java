package com.instaclustr.icarus.operations.rebuild;


import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;

import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target({TYPE, PARAMETER})
@Retention(RUNTIME)
@Constraint(validatedBy = {
        ValidRebuildOperationRequest.RebuildOperationRequestValidator.class,
})
public @interface ValidRebuildOperationRequest {

    String message() default "{com.instaclustr.icarus.operations.rebuild.ValidRebuildOperationRequest.message}";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

    final class RebuildOperationRequestValidator implements ConstraintValidator<ValidRebuildOperationRequest, RebuildOperationRequest> {
        @Override
        public boolean isValid(RebuildOperationRequest value, ConstraintValidatorContext context) {

            context.disableDefaultConstraintViolation();

            if (value.keyspace == null && value.specificTokens != null && !value.specificTokens.isEmpty()) {
                context
                        .buildConstraintViolationWithTemplate("{com.instaclustr.icarus.operations.rebuild.ValidRebuildOperationRequest.keyspaceMissingForSpecificTokens}")
                        .addConstraintViolation();

                return false;
            }

            return true;
        }
    }
}
