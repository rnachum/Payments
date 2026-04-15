package com.example.payment;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.util.Optional;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = PaymentController.class)
@Import(GlobalExceptionHandler.class)
class PaymentControllerWebMvcTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private PaymentService paymentService;

    @Test
    void createPaymentReturns201AndLocation() throws Exception {
        when(paymentService.createPayment(any(CreatePaymentRequest.class), eq("idem-1")))
                .thenReturn(new AcceptedPaymentResponse(42L, PaymentStatus.PENDING));

        mockMvc.perform(
                        post("/payments")
                                .header("Idempotency-Key", "idem-1")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content("{\"amount\":99.50,\"currency\":\"USD\"}"))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", containsString("/payments/42")))
                .andExpect(jsonPath("$.id").value(42))
                .andExpect(jsonPath("$.status").value("PENDING"));
    }

    @Test
    void negativeAmountReturns400() throws Exception {
        mockMvc.perform(
                        post("/payments")
                                .header("Idempotency-Key", "idem-neg")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content("{\"amount\":-5,\"currency\":\"USD\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("VALIDATION_FAILED"))
                .andExpect(jsonPath("$.fieldErrors.amount").exists());

        verify(paymentService, never()).createPayment(any(CreatePaymentRequest.class), anyString());
    }

    @Test
    void invalidCurrencyReturns400() throws Exception {
        mockMvc.perform(
                        post("/payments")
                                .header("Idempotency-Key", "idem-ccy")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content("{\"amount\":10,\"currency\":\"us\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("VALIDATION_FAILED"))
                .andExpect(jsonPath("$.fieldErrors.currency").exists());

        verify(paymentService, never()).createPayment(any(CreatePaymentRequest.class), anyString());
    }

    @Test
    void missingIdempotencyKeyReturns400() throws Exception {
        mockMvc.perform(
                        post("/payments")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content("{\"amount\":1,\"currency\":\"USD\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Idempotency-Key header is required"));
    }

    @Test
    void getPaymentReturns200WhenFound() throws Exception {
        when(paymentService.getPayment(7L))
                .thenReturn(
                        Optional.of(new PaymentDetailsResponse(7L, BigDecimal.TEN, "EUR", PaymentStatus.COMPLETED)));

        mockMvc.perform(get("/payments/7"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(7))
                .andExpect(jsonPath("$.currency").value("EUR"))
                .andExpect(jsonPath("$.status").value("COMPLETED"));
    }

    @Test
    void getPaymentReturns404WhenMissing() throws Exception {
        when(paymentService.getPayment(99L)).thenReturn(Optional.empty());

        mockMvc.perform(get("/payments/99")).andExpect(status().isNotFound());
    }

    @Test
    void healthReturnsUp() throws Exception {
        mockMvc.perform(get("/payments/health"))
                .andExpect(status().isOk())
                .andExpect(content().json("{\"status\":\"UP\"}"));
    }
}
