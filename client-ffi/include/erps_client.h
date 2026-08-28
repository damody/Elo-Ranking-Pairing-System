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
ERPS_API uint32_t erps_abi_version(void);
ERPS_API int32_t erps_client_create(const char *endpoint,const char *auth_token,ErpsClient **out_client);
ERPS_API int32_t erps_client_create_tls(const char *endpoint,const char *auth_token,const char *tls_domain,ErpsClient **out_client);
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
ERPS_API const char *erps_event_entity_id(const ErpsEvent *event);
ERPS_API uint64_t erps_event_revision(const ErpsEvent *event);
ERPS_API const char *erps_event_endpoint(const ErpsEvent *event);
ERPS_API const char *erps_event_connection_token(const ErpsEvent *event);
ERPS_API const char *erps_event_party_id(const ErpsEvent *event);
ERPS_API const char *erps_event_ticket_id(const ErpsEvent *event);
ERPS_API const char *erps_event_proposal_id(const ErpsEvent *event);
ERPS_API const char *erps_event_match_id(const ErpsEvent *event);
/* Match roster pointers remain valid until erps_event_release(event). */
ERPS_API size_t erps_event_team_count(const ErpsEvent *event);
ERPS_API size_t erps_event_team_player_count(const ErpsEvent *event,size_t team_index);
ERPS_API const char *erps_event_team_player_id(const ErpsEvent *event,size_t team_index,size_t player_index);
ERPS_API void erps_event_release(ErpsEvent *event);
#ifdef __cplusplus
}
#endif
#endif
