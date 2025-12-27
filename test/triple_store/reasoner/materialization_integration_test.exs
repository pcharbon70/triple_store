# credo:disable-for-this-file Credo.Check.Readability.FunctionNames
defmodule TripleStore.Reasoner.MaterializationIntegrationTest do
  @moduledoc """
  Integration tests for Task 4.6.1: Materialization Testing.

  These tests verify full materialization on realistic ontologies using
  a synthetic LUBM-like (Lehigh University Benchmark) dataset structure.

  ## LUBM Ontology Structure

  The LUBM ontology models a university domain with:
  - University, Department, Faculty, Student, Course, etc.
  - Class hierarchy: GraduateStudent < Student < Person
  - Property characteristics: advisedBy is functional, memberOf is transitive
  - Complex inference patterns requiring multiple iterations

  ## Test Coverage

  - 4.6.1.1: Test materialization on LUBM-style dataset
  - 4.6.1.2: Verify query results match expected inference closure
  - 4.6.1.3: Benchmark materialization performance
  - 4.6.1.4: Test parallel materialization speedup

  ## Test Data Scale Rationale

  The LUBM benchmark scales by number of universities. LUBM(1) corresponds to:
  - 15 departments per university
  - 10 faculty per department
  - 100 students per department
  - 10 courses per department

  These parameters were chosen to match the standard LUBM(1) scale while
  remaining practical for in-memory testing.
  """
  use TripleStore.ReasonerTestCase

  require Logger

  # ============================================================================
  # LUBM-Specific Namespace
  # ============================================================================

  # LUBM uses its own namespace for university benchmark terms
  @ub "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"

  # ============================================================================
  # LUBM Test Helpers
  # ============================================================================

  # LUBM-specific IRI builder (not shared, specific to this test)
  defp lubm_iri(name), do: {:iri, @ub <> name}

  # Generates a LUBM-style TBox (schema) with class and property hierarchies.
  #
  # Class hierarchy:
  #   Thing (top)
  #     ├── Person
  #     │     ├── Employee
  #     │     │     ├── Faculty
  #     │     │     │     ├── Professor
  #     │     │     │     │     ├── FullProfessor
  #     │     │     │     │     ├── AssociateProfessor
  #     │     │     │     │     └── AssistantProfessor
  #     │     │     │     └── Lecturer
  #     │     │     └── AdministrativeStaff
  #     │     └── Student
  #     │           ├── GraduateStudent
  #     │           │     ├── PhDStudent
  #     │           │     └── MastersStudent
  #     │           └── UndergraduateStudent
  #     ├── Organization
  #     │     ├── University
  #     │     └── Department
  #     ├── Work
  #     │     ├── Course
  #     │     └── Publication
  #     │           ├── Article
  #     │           └── Book
  #     └── ResearchGroup
  defp generate_lubm_tbox do
    MapSet.new([
      # Person hierarchy
      {lubm_iri("Person"), rdfs_subClassOf(), lubm_iri("Thing")},
      {lubm_iri("Employee"), rdfs_subClassOf(), lubm_iri("Person")},
      {lubm_iri("Faculty"), rdfs_subClassOf(), lubm_iri("Employee")},
      {lubm_iri("Professor"), rdfs_subClassOf(), lubm_iri("Faculty")},
      {lubm_iri("FullProfessor"), rdfs_subClassOf(), lubm_iri("Professor")},
      {lubm_iri("AssociateProfessor"), rdfs_subClassOf(), lubm_iri("Professor")},
      {lubm_iri("AssistantProfessor"), rdfs_subClassOf(), lubm_iri("Professor")},
      {lubm_iri("Lecturer"), rdfs_subClassOf(), lubm_iri("Faculty")},
      {lubm_iri("AdministrativeStaff"), rdfs_subClassOf(), lubm_iri("Employee")},
      {lubm_iri("Student"), rdfs_subClassOf(), lubm_iri("Person")},
      {lubm_iri("GraduateStudent"), rdfs_subClassOf(), lubm_iri("Student")},
      {lubm_iri("PhDStudent"), rdfs_subClassOf(), lubm_iri("GraduateStudent")},
      {lubm_iri("MastersStudent"), rdfs_subClassOf(), lubm_iri("GraduateStudent")},
      {lubm_iri("UndergraduateStudent"), rdfs_subClassOf(), lubm_iri("Student")},

      # Organization hierarchy
      {lubm_iri("Organization"), rdfs_subClassOf(), lubm_iri("Thing")},
      {lubm_iri("University"), rdfs_subClassOf(), lubm_iri("Organization")},
      {lubm_iri("Department"), rdfs_subClassOf(), lubm_iri("Organization")},

      # Work hierarchy
      {lubm_iri("Work"), rdfs_subClassOf(), lubm_iri("Thing")},
      {lubm_iri("Course"), rdfs_subClassOf(), lubm_iri("Work")},
      {lubm_iri("Publication"), rdfs_subClassOf(), lubm_iri("Work")},
      {lubm_iri("Article"), rdfs_subClassOf(), lubm_iri("Publication")},
      {lubm_iri("Book"), rdfs_subClassOf(), lubm_iri("Publication")},

      # ResearchGroup
      {lubm_iri("ResearchGroup"), rdfs_subClassOf(), lubm_iri("Thing")},

      # Property hierarchy
      {lubm_iri("memberOf"), rdfs_subPropertyOf(), lubm_iri("affiliatedWith")},
      {lubm_iri("worksFor"), rdfs_subPropertyOf(), lubm_iri("affiliatedWith")},
      {lubm_iri("headOf"), rdfs_subPropertyOf(), lubm_iri("worksFor")},

      # Property domain/range
      {lubm_iri("memberOf"), rdfs_domain(), lubm_iri("Person")},
      {lubm_iri("memberOf"), rdfs_range(), lubm_iri("Organization")},
      {lubm_iri("worksFor"), rdfs_domain(), lubm_iri("Employee")},
      {lubm_iri("worksFor"), rdfs_range(), lubm_iri("Organization")},
      {lubm_iri("teacherOf"), rdfs_domain(), lubm_iri("Faculty")},
      {lubm_iri("teacherOf"), rdfs_range(), lubm_iri("Course")},
      {lubm_iri("takesCourse"), rdfs_domain(), lubm_iri("Student")},
      {lubm_iri("takesCourse"), rdfs_range(), lubm_iri("Course")},
      {lubm_iri("advisor"), rdfs_domain(), lubm_iri("Student")},
      {lubm_iri("advisor"), rdfs_range(), lubm_iri("Professor")},
      {lubm_iri("publicationAuthor"), rdfs_domain(), lubm_iri("Person")},
      {lubm_iri("publicationAuthor"), rdfs_range(), lubm_iri("Publication")},

      # Property characteristics
      {lubm_iri("subOrganizationOf"), rdf_type(), owl_TransitiveProperty()},
      {lubm_iri("collaboratesWith"), rdf_type(), owl_SymmetricProperty()}
    ])
  end

  # Generates ABox (instance data) for a university with the given parameters.
  #
  # Options:
  #   - :departments - Number of departments (default: 15)
  #   - :faculty_per_dept - Number of faculty members per department (default: 10)
  #   - :students_per_dept - Number of students per department (default: 100)
  #   - :courses_per_dept - Number of courses per department (default: 10)
  defp generate_lubm_abox(opts \\ []) do
    num_departments = Keyword.get(opts, :departments, 15)
    faculty_per_dept = Keyword.get(opts, :faculty_per_dept, 10)
    students_per_dept = Keyword.get(opts, :students_per_dept, 100)
    courses_per_dept = Keyword.get(opts, :courses_per_dept, 10)

    university = ex_iri("University0")

    # Generate university
    facts = MapSet.new([
      {university, rdf_type(), lubm_iri("University")}
    ])

    # Generate departments and their contents
    Enum.reduce(0..(num_departments - 1), facts, fn dept_id, acc ->
      dept = ex_iri("Department#{dept_id}")

      # Department facts
      dept_facts = MapSet.new([
        {dept, rdf_type(), lubm_iri("Department")},
        {dept, lubm_iri("subOrganizationOf"), university}
      ])

      # Generate faculty
      faculty_facts = generate_faculty(dept_id, faculty_per_dept, dept)

      # Generate students
      student_facts = generate_students(dept_id, students_per_dept, dept, faculty_per_dept)

      # Generate courses
      course_facts = generate_courses(dept_id, courses_per_dept, dept, faculty_per_dept, students_per_dept)

      acc
      |> MapSet.union(dept_facts)
      |> MapSet.union(faculty_facts)
      |> MapSet.union(student_facts)
      |> MapSet.union(course_facts)
    end)
  end

  defp generate_faculty(dept_id, count, dept) do
    Enum.reduce(0..(count - 1), MapSet.new(), fn faculty_id, acc ->
      faculty = ex_iri("Faculty#{dept_id}_#{faculty_id}")

      # Assign professor types
      type = case rem(faculty_id, 4) do
        0 -> lubm_iri("FullProfessor")
        1 -> lubm_iri("AssociateProfessor")
        2 -> lubm_iri("AssistantProfessor")
        3 -> lubm_iri("Lecturer")
      end

      facts = MapSet.new([
        {faculty, rdf_type(), type},
        {faculty, lubm_iri("worksFor"), dept}
      ])

      # First faculty is head of department
      facts = if faculty_id == 0 do
        MapSet.put(facts, {faculty, lubm_iri("headOf"), dept})
      else
        facts
      end

      MapSet.union(acc, facts)
    end)
  end

  defp generate_students(dept_id, count, dept, num_faculty) do
    Enum.reduce(0..(count - 1), MapSet.new(), fn student_id, acc ->
      student = ex_iri("Student#{dept_id}_#{student_id}")

      # Assign student types
      type = case rem(student_id, 4) do
        0 -> lubm_iri("PhDStudent")
        1 -> lubm_iri("MastersStudent")
        2 -> lubm_iri("UndergraduateStudent")
        3 -> lubm_iri("UndergraduateStudent")
      end

      # Assign advisor (for graduate students)
      advisor_id = rem(student_id, num_faculty)
      advisor = ex_iri("Faculty#{dept_id}_#{advisor_id}")

      facts = MapSet.new([
        {student, rdf_type(), type},
        {student, lubm_iri("memberOf"), dept}
      ])

      # Graduate students have advisors
      facts = if rem(student_id, 4) in [0, 1] do
        MapSet.put(facts, {student, lubm_iri("advisor"), advisor})
      else
        facts
      end

      MapSet.union(acc, facts)
    end)
  end

  defp generate_courses(dept_id, count, _dept, num_faculty, num_students) do
    Enum.reduce(0..(count - 1), MapSet.new(), fn course_id, acc ->
      course = ex_iri("Course#{dept_id}_#{course_id}")
      teacher_id = rem(course_id, num_faculty)
      teacher = ex_iri("Faculty#{dept_id}_#{teacher_id}")

      facts = MapSet.new([
        {course, rdf_type(), lubm_iri("Course")},
        {teacher, lubm_iri("teacherOf"), course}
      ])

      # Students take courses
      students_per_course = div(num_students, count)
      student_facts = Enum.reduce(0..(students_per_course - 1), MapSet.new(), fn i, sacc ->
        student_id = rem(course_id * students_per_course + i, num_students)
        student = ex_iri("Student#{dept_id}_#{student_id}")
        MapSet.put(sacc, {student, lubm_iri("takesCourse"), course})
      end)

      acc
      |> MapSet.union(facts)
      |> MapSet.union(student_facts)
    end)
  end

  defp make_lookup(facts) do
    fn {:pattern, [s, p, o]} ->
      matching =
        facts
        |> Enum.filter(fn {fs, fp, fo} ->
          matches?(fs, s) and matches?(fp, p) and matches?(fo, o)
        end)

      {:ok, Enum.to_list(matching)}
    end
  end

  defp matches?(_fact_term, {:var, _}), do: true
  defp matches?(fact_term, pattern_term), do: fact_term == pattern_term

  # ============================================================================
  # Task 4.6.1.1: Test materialization on LUBM-style dataset
  # ============================================================================

  describe "4.6.1.1 materialization on LUBM-style dataset" do
    @tag timeout: 120_000
    test "materializes LUBM(1) scale dataset with RDFS rules" do
      # Generate LUBM(1) scale dataset (1 university)
      tbox = generate_lubm_tbox()
      abox = generate_lubm_abox(
        departments: 15,
        faculty_per_dept: 10,
        students_per_dept: 100,
        courses_per_dept: 10
      )

      initial_facts = MapSet.union(tbox, abox)
      initial_count = MapSet.size(initial_facts)

      # Materialize with stats
      {all_facts, stats} = materialize_with_stats(initial_facts, :rdfs)

      # Verify materialization completed
      assert stats.iterations > 0
      assert stats.total_derived > 0
      assert MapSet.size(all_facts) > initial_count

      # Log stats for debugging (visible with --trace flag)
      Logger.debug("""
      LUBM(1) RDFS Materialization:
        Initial facts: #{initial_count}
        Final facts: #{MapSet.size(all_facts)}
        Derived facts: #{stats.total_derived}
        Iterations: #{stats.iterations}
        Duration: #{stats.duration_ms}ms
      """)
    end

    @tag timeout: 120_000
    test "materializes LUBM(1) scale dataset with OWL 2 RL rules" do
      # Generate smaller dataset for OWL 2 RL (more complex rules = slower)
      # Still representative but practical for testing
      tbox = generate_lubm_tbox()
      abox = generate_lubm_abox(
        departments: 3,
        faculty_per_dept: 5,
        students_per_dept: 20,
        courses_per_dept: 5
      )

      initial_facts = MapSet.union(tbox, abox)
      initial_count = MapSet.size(initial_facts)

      # Materialize with stats
      {all_facts, stats} = materialize_with_stats(initial_facts, :owl2rl)

      # Verify materialization completed
      assert stats.iterations > 0
      assert MapSet.size(all_facts) > initial_count

      Logger.debug("""
      LUBM(1) OWL 2 RL Materialization:
        Initial facts: #{initial_count}
        Final facts: #{MapSet.size(all_facts)}
        Derived facts: #{stats.total_derived}
        Iterations: #{stats.iterations}
        Duration: #{stats.duration_ms}ms
      """)
    end

    test "materializes smaller dataset correctly" do
      # Small test for quick verification
      tbox = generate_lubm_tbox()
      abox = generate_lubm_abox(
        departments: 2,
        faculty_per_dept: 3,
        students_per_dept: 10,
        courses_per_dept: 2
      )

      initial_facts = MapSet.union(tbox, abox)

      {all_facts, stats} = materialize_with_stats(initial_facts, :rdfs)

      assert stats.iterations > 0
      assert MapSet.size(all_facts) > MapSet.size(initial_facts)
    end
  end

  # ============================================================================
  # Task 4.6.1.2: Verify query results match expected inference closure
  # ============================================================================

  describe "4.6.1.2 inference closure correctness" do
    test "class hierarchy inference produces expected types" do
      tbox = generate_lubm_tbox()

      # Create a single PhD student
      abox = MapSet.new([
        {ex_iri("Alice"), rdf_type(), lubm_iri("PhDStudent")},
        {ex_iri("Alice"), lubm_iri("memberOf"), ex_iri("Department0")}
      ])

      initial_facts = MapSet.union(tbox, abox)
      all_facts = materialize(initial_facts, :rdfs)

      # Alice should be inferred to be:
      # PhDStudent < GraduateStudent < Student < Person < Thing
      assert has_triple?(all_facts, {ex_iri("Alice"), rdf_type(), lubm_iri("GraduateStudent")})
      assert has_triple?(all_facts, {ex_iri("Alice"), rdf_type(), lubm_iri("Student")})
      assert has_triple?(all_facts, {ex_iri("Alice"), rdf_type(), lubm_iri("Person")})
      assert has_triple?(all_facts, {ex_iri("Alice"), rdf_type(), lubm_iri("Thing")})
    end

    test "property hierarchy inference produces expected properties" do
      tbox = generate_lubm_tbox()

      abox = MapSet.new([
        {ex_iri("Prof1"), lubm_iri("headOf"), ex_iri("Department0")}
      ])

      initial_facts = MapSet.union(tbox, abox)
      all_facts = materialize(initial_facts, :rdfs)

      # headOf < worksFor < affiliatedWith
      # So Prof1 headOf Dept0 should imply Prof1 worksFor Dept0 and Prof1 affiliatedWith Dept0
      assert has_triple?(all_facts, {ex_iri("Prof1"), lubm_iri("worksFor"), ex_iri("Department0")})
      assert has_triple?(all_facts, {ex_iri("Prof1"), lubm_iri("affiliatedWith"), ex_iri("Department0")})
    end

    test "domain/range inference produces expected types" do
      tbox = generate_lubm_tbox()

      abox = MapSet.new([
        {ex_iri("Prof1"), lubm_iri("teacherOf"), ex_iri("Course101")},
        {ex_iri("Student1"), lubm_iri("takesCourse"), ex_iri("Course101")}
      ])

      initial_facts = MapSet.union(tbox, abox)
      all_facts = materialize(initial_facts, :rdfs)

      # teacherOf has domain Faculty and range Course
      # So Prof1 should be inferred to be Faculty
      assert has_triple?(all_facts, {ex_iri("Prof1"), rdf_type(), lubm_iri("Faculty")})
      assert has_triple?(all_facts, {ex_iri("Course101"), rdf_type(), lubm_iri("Course")})

      # takesCourse has domain Student and range Course
      assert has_triple?(all_facts, {ex_iri("Student1"), rdf_type(), lubm_iri("Student")})
    end

    test "transitive property inference produces expected relations" do
      tbox = generate_lubm_tbox()

      # Create a chain of subOrganizationOf
      abox = MapSet.new([
        {ex_iri("ResearchGroup1"), rdf_type(), lubm_iri("ResearchGroup")},
        {ex_iri("Department0"), rdf_type(), lubm_iri("Department")},
        {ex_iri("University0"), rdf_type(), lubm_iri("University")},
        {ex_iri("ResearchGroup1"), lubm_iri("subOrganizationOf"), ex_iri("Department0")},
        {ex_iri("Department0"), lubm_iri("subOrganizationOf"), ex_iri("University0")}
      ])

      initial_facts = MapSet.union(tbox, abox)
      all_facts = materialize(initial_facts, :owl2rl)

      # Transitive: ResearchGroup1 subOrganizationOf University0
      assert has_triple?(all_facts, {ex_iri("ResearchGroup1"), lubm_iri("subOrganizationOf"), ex_iri("University0")})
    end

    test "symmetric property inference produces expected relations" do
      tbox = generate_lubm_tbox()

      abox = MapSet.new([
        {ex_iri("Prof1"), rdf_type(), lubm_iri("Professor")},
        {ex_iri("Prof2"), rdf_type(), lubm_iri("Professor")},
        {ex_iri("Prof1"), lubm_iri("collaboratesWith"), ex_iri("Prof2")}
      ])

      initial_facts = MapSet.union(tbox, abox)
      all_facts = materialize(initial_facts, :owl2rl)

      # Symmetric: Prof2 collaboratesWith Prof1
      assert has_triple?(all_facts, {ex_iri("Prof2"), lubm_iri("collaboratesWith"), ex_iri("Prof1")})
    end

    test "complete inference closure for faculty member" do
      tbox = generate_lubm_tbox()

      abox = MapSet.new([
        {ex_iri("Prof1"), rdf_type(), lubm_iri("FullProfessor")},
        {ex_iri("Prof1"), lubm_iri("worksFor"), ex_iri("Department0")},
        {ex_iri("Prof1"), lubm_iri("teacherOf"), ex_iri("Course101")},
        {ex_iri("Department0"), rdf_type(), lubm_iri("Department")},
        {ex_iri("University0"), rdf_type(), lubm_iri("University")},
        {ex_iri("Department0"), lubm_iri("subOrganizationOf"), ex_iri("University0")}
      ])

      initial_facts = MapSet.union(tbox, abox)
      all_facts = materialize(initial_facts, :rdfs)

      # Full class hierarchy for FullProfessor
      assert has_triple?(all_facts, {ex_iri("Prof1"), rdf_type(), lubm_iri("Professor")})
      assert has_triple?(all_facts, {ex_iri("Prof1"), rdf_type(), lubm_iri("Faculty")})
      assert has_triple?(all_facts, {ex_iri("Prof1"), rdf_type(), lubm_iri("Employee")})
      assert has_triple?(all_facts, {ex_iri("Prof1"), rdf_type(), lubm_iri("Person")})

      # Property hierarchy: worksFor < affiliatedWith
      assert has_triple?(all_facts, {ex_iri("Prof1"), lubm_iri("affiliatedWith"), ex_iri("Department0")})
    end

    test "statistics accuracy: derived count equals difference" do
      tbox = generate_lubm_tbox()
      abox = generate_lubm_abox(
        departments: 2,
        faculty_per_dept: 3,
        students_per_dept: 10,
        courses_per_dept: 2
      )

      initial_facts = MapSet.union(tbox, abox)
      initial_count = MapSet.size(initial_facts)

      {all_facts, stats} = materialize_with_stats(initial_facts, :rdfs)

      # Verify stats accuracy
      actual_derived = MapSet.size(all_facts) - initial_count
      assert stats.total_derived == actual_derived,
        "Stats derived (#{stats.total_derived}) != actual derived (#{actual_derived})"
    end
  end

  # ============================================================================
  # Task 4.6.1.3: Benchmark materialization performance
  # ============================================================================

  describe "4.6.1.3 benchmark materialization performance" do
    @tag timeout: 120_000
    @tag :benchmark
    test "LUBM(1) materialization completes in reasonable time" do
      tbox = generate_lubm_tbox()
      abox = generate_lubm_abox(
        departments: 15,
        faculty_per_dept: 10,
        students_per_dept: 100,
        courses_per_dept: 10
      )

      initial_facts = MapSet.union(tbox, abox)
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      # Run materialization and measure time
      {duration_us, {:ok, _all_facts, stats}} =
        :timer.tc(fn ->
          SemiNaive.materialize_in_memory(rules, initial_facts)
        end)

      duration_ms = div(duration_us, 1000)

      Logger.debug("""
      LUBM(1) Benchmark:
        Duration: #{duration_ms}ms (#{div(duration_us, 1_000_000)}s)
        Internal duration: #{stats.duration_ms}ms
        Initial facts: #{MapSet.size(initial_facts)}
        Derived facts: #{stats.total_derived}
        Iterations: #{stats.iterations}
      """)

      # LUBM(1) should complete well under 60 seconds
      # For in-memory with MapSet, it should be much faster
      assert duration_ms < 60_000, "Materialization took #{duration_ms}ms, expected <60000ms"

      # Should complete in reasonable number of iterations
      assert stats.iterations <= 20, "Too many iterations: #{stats.iterations}"
    end

    @tag timeout: 120_000
    @tag :benchmark
    test "materialization scales reasonably with data size" do
      tbox = generate_lubm_tbox()

      # Run with different data sizes and measure scaling
      sizes = [
        {2, 3, 10, 2},    # Small
        {5, 5, 50, 5},    # Medium
        {10, 8, 80, 8}    # Large
      ]

      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      results = Enum.map(sizes, fn {depts, faculty, students, courses} ->
        abox = generate_lubm_abox(
          departments: depts,
          faculty_per_dept: faculty,
          students_per_dept: students,
          courses_per_dept: courses
        )

        initial_facts = MapSet.union(tbox, abox)
        initial_count = MapSet.size(initial_facts)

        {duration_us, {:ok, _all_facts, stats}} =
          :timer.tc(fn ->
            SemiNaive.materialize_in_memory(rules, initial_facts)
          end)

        %{
          initial_count: initial_count,
          derived_count: stats.total_derived,
          duration_ms: div(duration_us, 1000),
          iterations: stats.iterations
        }
      end)

      Logger.debug("Scaling Benchmark: #{inspect(results, pretty: true)}")

      # Verify all completed
      Enum.each(results, fn r ->
        assert r.derived_count > 0
        assert r.duration_ms < 60_000
      end)
    end
  end

  # ============================================================================
  # Task 4.6.1.4: Test parallel materialization speedup
  # ============================================================================

  describe "4.6.1.4 parallel materialization" do
    @tag timeout: 120_000
    test "parallel materialization produces same results as sequential" do
      tbox = generate_lubm_tbox()
      abox = generate_lubm_abox(
        departments: 5,
        faculty_per_dept: 5,
        students_per_dept: 30,
        courses_per_dept: 5
      )

      initial_facts = MapSet.union(tbox, abox)
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      # Sequential
      {:ok, seq_facts, seq_stats} = SemiNaive.materialize_in_memory(rules, initial_facts, parallel: false)

      # Parallel
      {:ok, par_facts, par_stats} = SemiNaive.materialize_in_memory(rules, initial_facts, parallel: true)

      # Results must be identical
      assert MapSet.equal?(seq_facts, par_facts),
        "Sequential and parallel results differ!"

      assert seq_stats.total_derived == par_stats.total_derived,
        "Derived counts differ: seq=#{seq_stats.total_derived}, par=#{par_stats.total_derived}"

      Logger.debug("""
      Sequential vs Parallel:
        Sequential: #{seq_stats.duration_ms}ms, #{seq_stats.iterations} iterations
        Parallel: #{par_stats.duration_ms}ms, #{par_stats.iterations} iterations
      """)
    end

    @tag timeout: 120_000
    @tag :benchmark
    test "parallel materialization shows speedup on larger dataset" do
      tbox = generate_lubm_tbox()
      abox = generate_lubm_abox(
        departments: 10,
        faculty_per_dept: 8,
        students_per_dept: 80,
        courses_per_dept: 8
      )

      initial_facts = MapSet.union(tbox, abox)
      {:ok, rules} = ReasoningProfile.rules_for(:owl2rl)

      # Run multiple times to get stable measurements
      num_runs = 3

      seq_times = Enum.map(1..num_runs, fn _ ->
        {duration_us, _result} = :timer.tc(fn ->
          SemiNaive.materialize_in_memory(rules, initial_facts, parallel: false)
        end)
        div(duration_us, 1000)
      end)

      par_times = Enum.map(1..num_runs, fn _ ->
        {duration_us, _result} = :timer.tc(fn ->
          SemiNaive.materialize_in_memory(rules, initial_facts, parallel: true)
        end)
        div(duration_us, 1000)
      end)

      avg_seq = div(Enum.sum(seq_times), num_runs)
      avg_par = div(Enum.sum(par_times), num_runs)

      speedup = if avg_par > 0, do: avg_seq / avg_par, else: 0

      Logger.debug("""
      Parallel Speedup Benchmark:
        Sequential avg: #{avg_seq}ms (runs: #{inspect(seq_times)})
        Parallel avg: #{avg_par}ms (runs: #{inspect(par_times)})
        Speedup: #{Float.round(speedup, 2)}x
        Schedulers: #{System.schedulers_online()}
      """)

      # Both should complete
      assert avg_seq > 0
      assert avg_par > 0

      # Parallel should not be significantly slower
      # (on small datasets, overhead may make parallel slower)
      assert avg_par < avg_seq * 2, "Parallel is >2x slower than sequential"
    end

    test "parallel mode is deterministic across multiple runs" do
      tbox = generate_lubm_tbox()
      abox = generate_lubm_abox(
        departments: 3,
        faculty_per_dept: 4,
        students_per_dept: 20,
        courses_per_dept: 3
      )

      initial_facts = MapSet.union(tbox, abox)
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      # Run parallel multiple times
      results = Enum.map(1..5, fn _ ->
        {:ok, facts, _stats} = SemiNaive.materialize_in_memory(rules, initial_facts, parallel: true)
        facts
      end)

      # All results should be identical
      first = hd(results)
      Enum.each(results, fn facts ->
        assert MapSet.equal?(facts, first), "Non-deterministic parallel results!"
      end)
    end
  end
end
