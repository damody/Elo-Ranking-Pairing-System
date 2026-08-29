#ifndef ERPS_CLIENT_H
#define ERPS_CLIENT_H
#include <stdint.h>
#include <stddef.h>
#if defined(_WIN32)
#define ERPS_API __declspec(dllimport)
#else
#define ERPS_API __attribute__((visibility("default")))
#endif
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ErpsClient ErpsClient;
typedef struct ErpsEvent ErpsEvent;
enum { ERPS_OK=0, ERPS_NO_EVENT=1, ERPS_INVALID_ARGUMENT=-1, ERPS_RUNTIME_ERROR=-2, ERPS_PANIC=-3, ERPS_THREAD_MISUSE=-4 };
enum { ERPS_MODE_1V1=1, ERPS_MODE_5V5=2, ERPS_MODE_FFA8=3 };
enum { ERPS_EVENT_PARTY=1, ERPS_EVENT_PROPOSAL=2, ERPS_EVENT_MATCHED=3, ERPS_EVENT_SERVER_LOST=4, ERPS_EVENT_STATE=5, ERPS_EVENT_PROPOSAL_CANCELLED=6 };
ERPS_API uint32_t erps_abi_version(void);
ERPS_API int32_t erps_client_create(const char *endpoint,const char *auth_token,ErpsClient **out_client);
ERPS_API int32_t erps_client_create_tls(const char *endpoint,const char *auth_token,const char *tls_domain,ErpsClient **out_client);
/* Uses platform trust roots plus the supplied PEM-encoded private CA certificate. */
ERPS_API int32_t erps_client_create_tls_with_ca(const char *endpoint,const char *auth_token,const char *tls_domain,const char *ca_pem,ErpsClient **out_client);
ERPS_API void erps_client_destroy(ErpsClient *client);
ERPS_API int32_t erps_client_shutdown(ErpsClient *client);
ERPS_API int32_t erps_client_start_events(ErpsClient *client);
ERPS_API int32_t erps_client_create_party(ErpsClient *client,const char *utf8_name);
ERPS_API int32_t erps_client_create_invite(ErpsClient *client,const char *party_id,uint64_t revision,uint32_t ttl_seconds,uint32_t uses,char *out_token,size_t out_token_size);
ERPS_API int32_t erps_client_join_party(ErpsClient *client,const char *invite_token);
ERPS_API int32_t erps_client_leave_party(ErpsClient *client,const char *party_id,uint64_t revision);
ERPS_API int32_t erps_client_kick_member(ErpsClient *client,const char *party_id,uint64_t revision,const char *player_id);
ERPS_API int32_t erps_client_rename_party(ErpsClient *client,const char *party_id,uint64_t revision,const char *utf8_name);
ERPS_API int32_t erps_client_enqueue(ErpsClient *client,const char *party_id,uint64_t revision,uint32_t mode,const char *region);
ERPS_API int32_t erps_client_cancel_queue(ErpsClient *client,const char *party_id,uint64_t revision);
/* Enqueues an ERPS_EVENT_STATE snapshot for the poll consumer. */
ERPS_API int32_t erps_client_get_state(ErpsClient *client);
ERPS_API int32_t erps_client_accept(ErpsClient *client,const char *proposal_id);
ERPS_API int32_t erps_client_reject(ErpsClient *client,const char *proposal_id);
/* poll is bound to the first calling thread; another thread receives ERPS_THREAD_MISUSE. */
ERPS_API int32_t erps_client_poll(ErpsClient *client,ErpsEvent **out_event);
/* Pointer remains valid until the next failing operation on this client. */
ERPS_API const char *erps_client_last_error(const ErpsClient *client);
ERPS_API uint32_t erps_event_kind(const ErpsEvent *event);
/* Unix epoch milliseconds; nonzero only for ERPS_EVENT_PROPOSAL. */
ERPS_API int64_t erps_event_deadline_ms(const ErpsEvent *event);
ERPS_API const char *erps_event_entity_id(const ErpsEvent *event);
ERPS_API uint64_t erps_event_revision(const ErpsEvent *event);
ERPS_API const char *erps_event_endpoint(const ErpsEvent *event);
ERPS_API const char *erps_event_connection_token(const ErpsEvent *event);
ERPS_API const char *erps_event_party_id(const ErpsEvent *event);
ERPS_API const char *erps_event_ticket_id(const ErpsEvent *event);
ERPS_API const char *erps_event_proposal_id(const ErpsEvent *event);
ERPS_API const char *erps_event_match_id(const ErpsEvent *event);
/* Party strings and member data remain valid until erps_event_release(event). */
ERPS_API const char *erps_event_party_name(const ErpsEvent *event);
ERPS_API const char *erps_event_party_leader_id(const ErpsEvent *event);
ERPS_API const char *erps_event_party_state(const ErpsEvent *event);
ERPS_API size_t erps_event_party_member_count(const ErpsEvent *event);
ERPS_API const char *erps_event_party_member_id(const ErpsEvent *event,size_t member_index);
ERPS_API int32_t erps_event_party_member_rating(const ErpsEvent *event,size_t member_index);
/* Returns zero for an invalid ERPS_MODE_* value or member index. */
ERPS_API int32_t erps_event_party_member_rating_for_mode(const ErpsEvent *event,size_t member_index,uint32_t mode);
ERPS_API uint32_t erps_event_party_member_credit(const ErpsEvent *event,size_t member_index);
/* Player profile is populated on STATE; credit/reason/eligibility are also populated on PROPOSAL_CANCELLED. */
ERPS_API const char *erps_event_reason(const ErpsEvent *event);
ERPS_API int32_t erps_event_player_rating_for_mode(const ErpsEvent *event,uint32_t mode);
ERPS_API uint32_t erps_event_player_credit(const ErpsEvent *event);
ERPS_API uint32_t erps_event_player_eligible(const ErpsEvent *event);
ERPS_API int64_t erps_event_credit_suspended_until_ms(const ErpsEvent *event);
/* Queue mode/regions are populated on STATE; mode is also populated on MATCHED. */
ERPS_API uint32_t erps_event_queue_mode(const ErpsEvent *event);
ERPS_API size_t erps_event_allowed_region_count(const ErpsEvent *event);
ERPS_API const char *erps_event_allowed_region(const ErpsEvent *event,size_t region_index);
/* Match roster pointers remain valid until erps_event_release(event). */
ERPS_API size_t erps_event_team_count(const ErpsEvent *event);
ERPS_API size_t erps_event_team_player_count(const ErpsEvent *event,size_t team_index);
ERPS_API const char *erps_event_team_player_id(const ErpsEvent *event,size_t team_index,size_t player_index);
ERPS_API void erps_event_release(ErpsEvent *event);
#ifdef __cplusplus
}
#endif
#endif
