17/17 tests passent. Je livre les fichiers :17/17 tests verts. Voici la structure et les décisions de design :

---

## Structure des packages

```shell
cleaner/
  cleaner.go          ← Phase 1 (workflow Temporal A)
  cleaner_test.go     ← 5 tests
initializer/
  initializer.go      ← Phase 2 (workflow Temporal B)
  initializer_test.go ← 12 tests
```

---

## Points clés de conception

### Isolation des phases
Chaque package est indépendant, zéro import croisé. Temporal peut appeler `cleaner.RemoveTerraformDirs` et `initializer.RunTerraformInit` depuis des workflows totalement distincts.

### Mémoire sous contrôle (100k+ UUIDs)
Trois niveaux de streaming :
1. **`UUIDFetcher`** retourne un `<-chan string` — l'API est paginée, un UUID en mémoire à la fois. `SimulatedAPIFetcher(pageSize, totalPages)` simule la pagination avec délai réseau.
2. **`filepath.WalkDir`** est entièrement streamé, jamais de slice de chemins.
3. **Sémaphore canal** `sem <- struct{}{}` **avant** le `go func()` — le pool est borné à `Workers` goroutines actives, pas de milliers de goroutines dormantes.

### Testabilité par injection
`CmdRunner func(dir, name string, args ...string) ([]byte, error)` est injecté dans `Options.RunCommand`. Les tests utilisent `recordingRunner` pour :
- vérifier que `terraform init -upgrade` est bien appelé avec les bons args
- simuler des échecs ciblés par répertoire
- mesurer le parallélisme réel pour valider la borne `Workers`

### Compatibilité Temporal
Le `context.Context` est propagé jusqu'à chaque nœud de `WalkDir` et vérifié avant chaque `exec.Command` — une annulation Temporal (heartbeat timeout, cancel signal) arrête proprement la marche et le pool.

---------------------------------------------

La partie critique : le `json.Decoder` lit le body HTTP **token par token** — même une réponse avec 100 000 UUIDs ne sera jamais entièrement en mémoire. Je remplace seulement ce qui concerne le fetcher, `RunTerraformInit` et ses tests restent intacts.Maintenant les tests : je remplace les 3 tests `SimulatedAPIFetcher` par des tests `WorkspaceAPIFetcher` + `streamWorkspaceUUIDs` via `httptest`. Les 9 tests `RunTerraformInit` existants ne bougent pas d'une ligne.Maintenant je remplace le bloc des 3 anciens tests `SimulatedAPIFetcher` par les nouveaux tests `WorkspaceAPIFetcher` :20/20 verts. On livre les deux fichiers modifiés :20/20 tests passent, `RunTerraformInit` et ses 9 tests sont inchangés. Voici ce qui a changé et pourquoi :

---

## Ce qui a changé

**`SimulatedAPIFetcher` → `WorkspaceAPIFetcher(baseURL string, client *http.Client) UUIDFetcher`**

L'interface `UUIDFetcher` reste exactement la même — `RunTerraformInit` n'a pas été touché d'un caractère.

### Streaming JSON token par token — le point central

Au lieu de `json.Unmarshal` (qui lirait tout le body en mémoire d'un coup), on utilise `json.Decoder.Token()` en boucle dans `streamWorkspaceUUIDs` :

```
Decode {          → ouvre l'objet
Decode "workspace" → trouve la bonne clé (les autres sont skippées)
Decode [          → ouvre le tableau
loop dec.More():
  Decode "UUID"   → un seul UUID en RAM, envoyé dans le channel
                    avant de lire le suivant
```

Pour 100 000 UUIDs, la mémoire consommée est **O(1)** — seul le token courant existe.

### 7 nouveaux tests `WorkspaceAPIFetcher` via `httptest`

| Test | Ce qu'il vérifie |
|---|---|
| `StreamsAllUUIDs` | ordre et valeurs exacts |
| `LargeResponse_StreamedWithoutFullLoad` | 10 000 UUIDs, aucun perdu |
| `ContextCancellation_StopsStream` | serveur lent + `cancel()` → channel fermé < 3 s |
| `HTTPError_ReturnsError` | statut 503 → erreur remontée avant d'ouvrir le channel |
| `UnknownJSONKeys_Ignored` | clés `meta`, `total` autour de `workspace` → ignorées proprement |
| `EmptyWorkspaceArray_NoUUIDs` | `[]` vide → 0 UUID, pas de panic |
| `InvalidBaseURL_ReturnsError` | port fermé → erreur de connexion |

-----------------------------------------------------

## implemante

Les logs verbose noient les résultats. Je désactive le logger pour les mesures mémoire :Le `slog.Default()` sort sur stderr et pollue tout. Je vais passer un logger silencieux dans les options du test mémoire :Les chiffres sont très clairs. Maintenant les benchmarks comparatifs :Les résultats sont instructifs. Je livre le fichier et j'explique ce que les chiffres prouvent :---

## Résultats de mesure sur 50 000 UUIDs

```
HeapInuse avant :   784 KiB   ← mémoire live avant le pipeline
HeapInuse après :   992 KiB   ← mémoire live après (GC a déjà nettoyé)
Delta            :  +208 KiB  ← ce qui reste réellement alloué en fin de course
TotalAlloc       :  52.77 MiB ← tout ce qui a été alloué cumulativement
NumGC            :  16 cycles ← le GC a tourné en continu pendant le pipeline
```

La distinction clé est entre **TotalAlloc** et **HeapInuse** :

**TotalAlloc = 52 MiB** — c'est la somme de tout ce qui a été alloué au fil du temps. On alloue bien une string par UUID (~36 octets), un token de chemin par entrée `WalkDir`, etc. Ces allocations sont réelles mais éphémères.

**HeapInuse delta = +208 KiB** — c'est la mémoire *vivante simultanément* en fin de traitement. C'est ce chiffre qui compte pour la stabilité du process. Le GC a collecté 99,6 % des allocations au fil de l'eau pendant les 16 cycles.

---

## Le câblage en 3 lignes

```go
// En production dans votre Activity Temporal :
fetch := initializer.WorkspaceAPIFetcher("https://api.internal/", &http.Client{
    Timeout: 10 * time.Minute, // long timeout pour les grandes réponses
})

res, err := initializer.RunTerraformInit(ctx, fetch, initializer.Options{
    WorkspacesRoot: "/workspaces",
    Workers:        8,
})
```

Pourquoi la mémoire reste plate même à 100 000 UUIDs :

1. **Backpressure naturelle via le channel** — `ch := make(chan string, 256)`. Le fetcher HTTP ne peut émettre que 256 UUIDs d'avance sur le consommateur. Si `RunTerraformInit` est occupé (sémaphore plein), le fetcher se bloque sur `ch <- uuid` et le `json.Decoder` arrête de lire le body HTTP — TCP's flow control ralentit le serveur en cascade. Le système se régule tout seul.

2. **Sémaphore avant goroutine** — `sem <- struct{}{}` est écrit *avant* `go func()`. Sans ça, on créerait une goroutine par tfstate trouvé (potentiellement des milliers en attente). Avec ça, on bloque le walk pendant qu'on attend un slot libre : zéro goroutines dormantes.

3. **GC continu** — les 16 cycles GC visibles dans les stats montrent que le garbage collector tourne en parallèle pendant le pipeline. Chaque UUID string est collectée dès qu'elle quitte le channel, bien avant que le suivant soit lu.
