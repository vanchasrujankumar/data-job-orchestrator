package com.dataorchestrator.api.dto;

import java.util.List;

import org.springframework.data.domain.Page;

/**
 * Generic paginated response, avoiding direct serialisation of Spring's {@code PageImpl}.
 */
public record PageResponse<T>(
    List<T> content,
    int page,
    int size,
    long totalElements,
    int totalPages
) {
    /** Maps a Spring {@link Page} to the response model. */
    public static <T, R> PageResponse<R> of(Page<T> page, java.util.function.Function<T, R> mapper) {
        return new PageResponse<>(
            page.getContent().stream().map(mapper).toList(),
            page.getNumber(),
            page.getSize(),
            page.getTotalElements(),
            page.getTotalPages()
        );
    }
}
